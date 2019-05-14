package tag_stat

import (
	"dmp_web/go/model/model_rule"
	"fmt"
	"path/filepath"
	"runtime"

	"sync"

	"dmp_web/go/model"

	"context"

	"dmp_web/go/commons/db/hive"
	"dmp_web/go/commons/log"

	"dmp_web/go/commons/errors"

	"gopkg.in/mgo.v2/bson"
)

// Metric 统计器
type Metric struct {
	dimension              string
	Name                   string
	DateField              string
	SupplierDependProducts []model.Product // 查询该指标供应商限制所依赖的产品列表(DSP,DNA,DMP)
	etype                  []int           //pc 和 mobile 使用sql 的标示
	Layout                 string          // layout 中 %[1]v 为where日期条件(完整where语句), %[2]v 为人群表名, %[3]...等都是 供应商限制条件(只包含值)

}

// ProcessConfig 配置信息
type ProcessConfig struct {
	Date          *model_rule.Date
	Days          []string
	SuppliersStrs map[model.Product]string
	etype         int
}

// 获取所有供应商列表
func getSuppliersGroup(productToSuppliers map[model.Product]string) []string {
	suppliersGroup := []string{}
	for _, product := range productToSuppliers {
		suppliersGroup = append(suppliersGroup, product)
	}
	return suppliersGroup
}

// 获取 ProcessConfig 中有效的供应商类型
func getValidProducts(cfg *ProcessConfig) []model.Product {
	products := make([]model.Product, 0, len(cfg.SuppliersStrs))
	for product, suppliersStr := range cfg.SuppliersStrs {
		if suppliersStr != "" {
			products = append(products, product)
		}
	}
	return products
}

// 检查 metric 中 所依赖的供应商是否都存在
func checkSuppiersDepending(depending []model.Product, productToSuppliers map[model.Product]string) bool {
	for _, product := range depending {
		if productToSuppliers[product] == "" {
			return false
		}
	}
	return true
}

// baseProcessByDate　时间区间是时间段
func baseProcessByDate(ctx context.Context, fp string, p MetricProcessor, m *Metric, cfg *ProcessConfig) {
	if !exist(cfg.etype, m.etype) {
		return
	}

	date := cfg.Date
	statModel := p.GetModel()
	f := p.GetFunc(m.Name)
	suppliersGroup := getSuppliersGroup(cfg.SuppliersStrs)
	log.Debug(suppliersGroup)

	// p MetricProcessor 的 StatementByRange 方法是继承于 context 的
	sql, err := p.StatementByRange(m.Layout, m.DateField, date, suppliersGroup, m.Name)
	log.Debugf("enter baseProcessByteDate, %s,%s,%s", m.dimension, m.Name, sql)
	if err != nil {
		log.Error(err)
	}

	start, end := date.RangeString()
	tag := fmt.Sprintf("tag[%v], dt: (%v/%v)", p.TagId(), start, end)

	err = p.HiveCtx(ctx, sql, func(ret *hive.ExecuteResult) {
		// 并不要因为一个错误取消所有任务
		if !ret.WaitCtx(ctx) {
			// p.CancelAll(errors.New(ret.Err()))
			log.Errorf("%v %v error, %v -> : %v",
				fp, m.Name, tag, ret.Err())
			log.Error("Error SQL: ", sql)
			return
		}

		if err := removeStatData(statModel, m.Name, p.TagId(), date); err != nil {
			log.Error(err)
		}
		// NOTICE: 这是一个时间范围
		size, err := f(ctx, ret, start)
		if err != nil {
			log.Debugf("%v,%v error, %v -> %v: %v",
				fp, m.Name, tag, p.Progress(), err)
			return
		}
		log.Debugf("%v %v done, %v, size: %v -> %v",
			fp, m.Name, tag, size, p.Progress())
	})

	if err != nil {
		log.Debugf("%v,%v error, %v -> %v: %v",
			fp, m.Name, tag, p.Progress(), err)
		return
	}

	log.Debugf("%s, %v start, %v -> %v", fp, m.Name, tag, p.Progress())
}

func baseProcessByDateSql(p MetricProcessor, m *Metric, cfg *ProcessConfig) (string, error) {
	if !exist(cfg.etype, m.etype) {
		return "", nil
	}
	date := cfg.Date
	suppliersGroup := getSuppliersGroup(cfg.SuppliersStrs)
	//log.Debug(suppliersGroup)

	sql, err := p.StatementByRange(m.Layout, m.DateField, date, suppliersGroup, m.Name)
	if err != nil {
		return "", err
	}
	return sql, nil
}

// 移除mongo 旧数据
func removeStatData(statModel model.StatModel, metric string, tagId int64, date *model_rule.Date) error {
	start, end := date.RangeString()
	return statModel.RemoveAll(bson.M{
		"metric": metric,
		"TagId":  tagId,
		"ToDate": bson.M{
			"$gte": start,
			"$lte": end,
		},
	})
}

// 时间段内每一天的数据分别计算
func baseProcessDayByDay(ctx context.Context, fp string, p MetricProcessor, m *Metric, cfg *ProcessConfig) {
	if !exist(cfg.etype, m.etype) {
		return
	}

	dts := cfg.Days
	statModel := p.GetModel()
	f := p.GetFunc(m.Name)
	wg := ctx.Value("WG").(*sync.WaitGroup)
	suppliersGroup := getSuppliersGroup(cfg.SuppliersStrs)
	log.Debug(suppliersGroup)
	sqls, err := p.StatementsByDay(m.Layout, m.DateField, dts, suppliersGroup, m.Name)
	if err != nil {
		log.Error(err)
	}
	wg.Add(len(sqls))
	for idx, sql := range sqls {
		go func(idx int, sql string) {
			defer wg.Done()
			defer errors.HandlePanic()
			log.Debugf("enter baseProcessDayByDay %sql,%sql,%sql", m.dimension, m.Name, sql)
			var dt string
			if idx < len(dts) {
				dt = dts[idx]
			}
			tag := fmt.Sprintf("tag: %v, dt: %v", p.TagId(), dt)

			err = p.HiveCtx(ctx, sql, func(ret *hive.ExecuteResult) {
				if !ret.WaitCtx(ctx) {
					// p.CancelAll(errors.New(ret.Err()))
					log.Errorf("%v %v error, %v: %v",
						fp, m.Name, tag, ret.Err())
					return
				}
				statModel.Clean(m.Name, p.TagId(), dt)
				size, err := f(ctx, ret, dt)
				if err != nil {
					log.Debugf("%v,%v error, %v -> %v: %v",
						fp, m.Name, tag, p.Progress(), err)
					return
				}
				log.Debugf("%v %v done, %v, size: %v, -> %v",
					fp, m.Name, tag, size, p.Progress())
			})
			if err != nil {
				log.Debugf("%v,%v error, %v -> %v: %v",
					fp, m.Name, tag, p.Progress(), err)
				return
			}
			log.Debugf("%sql, %v start, %v -> %v", fp, m.Name, tag, p.Progress())
		}(idx, sql)
	}
}

func baseProcessDayByDaySqls(p MetricProcessor, m *Metric, cfg *ProcessConfig) ([]string, error) {
	if !exist(cfg.etype, m.etype) {
		return nil, errors.Wrap(errors.ErrType{cfg.etype}, "")
	}
	dts := cfg.Days
	suppliersGroup := getSuppliersGroup(cfg.SuppliersStrs)
	sqls, err := p.StatementsByDay(m.Layout, m.DateField, dts, suppliersGroup, m.Name)
	if err != nil {
		return nil, err
	}
	return sqls, nil
}

func baseProcessSqls(p MetricProcessor, ms []*Metric, cfg *ProcessConfig) ([]string, error) {
	if cfg == nil {
		cfg = new(ProcessConfig)
	}

	// 获取供应商 Id
	if cfg.SuppliersStrs == nil {
		cfg.SuppliersStrs = map[model.Product]string{}
	}

	if cfg.Date == nil && len(cfg.Days) == 0 {
		cfg.Date = p.GetDate()
	}
	if cfg.Date != nil {
		sqls := make([]string, 0, len(ms))

		// ms 是 Metric 统计查询的 SQL array
		for _, m := range ms {
			f := p.GetFunc(m.Name)
			if f == nil {
				return nil, errors.Wrap(errors.ErrType{m.Name}, "")
			}
			sql, err := baseProcessByDateSql(p, m, cfg)
			if err != nil {
				return nil, err
			}
			if sql != "" {
				sqls = append(sqls, sql)
			}
		}
		return sqls, nil
	} else {
		sqls := make([]string, len(ms))

		// ms 是 Metric 统计查询的 SQL array
		for _, m := range ms {
			f := p.GetFunc(m.Name)
			if f == nil {
				return nil, errors.Wrap(errors.ErrType{m.Name}, "")
			}
			sql, err := baseProcessDayByDaySqls(p, m, cfg)
			if err != nil {
				return nil, err
			}
			if len(sql) > 0 {
				for _, s := range sql {
					if s != "" {
						sqls = append(sqls, s)
					}
				}
			}
		}
		return sqls, nil
	}
}

//baseProcess 每一个统计维度都调用这个方法　process
func baseProcess(ctx context.Context, p MetricProcessor, ms []*Metric, cfg *ProcessConfig) {
	// 通过 runtime 获取运行文件的 fp
	_, fp, _, _ := runtime.Caller(1)
	fp = filepath.Base(fp)

	if cfg == nil {
		cfg = new(ProcessConfig)
	}

	// 获取供应商 Id
	if cfg.SuppliersStrs == nil {
		cfg.SuppliersStrs = map[model.Product]string{}
	}

	if cfg.Date == nil && len(cfg.Days) == 0 {
		cfg.Date = p.GetDate()
	}

	if cfg.Date != nil {

		// ms 是 Metric 统计查询的 SQL array
		for _, m := range ms {
			func(m *Metric) {
				if !checkSuppiersDepending(m.SupplierDependProducts, cfg.SuppliersStrs) {
					validProducts := getValidProducts(cfg)
					log.Debugf("%v %v depends on %v supplier, but tag %v provides %v supplier only. skipping...",
						fp, m.Name, m.SupplierDependProducts, p.TagId(), validProducts)
					return
				}
				f := p.GetFunc(m.Name)
				if f == nil {
					return
				}
				baseProcessByDate(ctx, fp, p, m, cfg)
			}(m)
		}
	} else {
		for _, m := range ms {
			func(m *Metric) {
				if !checkSuppiersDepending(m.SupplierDependProducts, cfg.SuppliersStrs) {
					validProducts := getValidProducts(cfg)
					log.Debugf("%v %v depends on %v supplier, but tag %v provides %v supplier only. skipping...",
						fp, m.Name, m.SupplierDependProducts, p.TagId(), validProducts)
					return
				}
				f := p.GetFunc(m.Name)
				if f == nil {
					return
				}
				baseProcessDayByDay(ctx, fp, p, m, cfg)
			}(m)
		}
	}
}

// 通用的 mongo selector 专门用于返回 DSP 任务的数据的唯一查询条件
func mongoSelector(campaignStat *AnalyseStat, metric string) bson.M {

	selector := bson.M{
		campaignStat.analyse.AnalyseName(): bson.M{"$in": campaignStat.analyse.GetIds()},
		"metric":                           metric,
		"ToDate": bson.M{
			"$gte": campaignStat.FromDate,
			"$lte": campaignStat.ToDate,
		},
	}
	return selector
}

// MetricProcessor 各个报表维度的需要实现接口
type MetricProcessor interface {
	GetModel() model.StatModel
	Process(ctx context.Context)
	ProcessSql() ([]string, error)
	TagId() int64
	GetDate() *model_rule.Date
	GetFunc(metric string) ProcessFunc
	StatementsByDay(layout string, field string, dts []string, suppliers []string, name string) ([]string, error)
	StatementByRange(layout string, field string, date *model_rule.Date, suppliers []string, name string) (string, error)
	Progress() string
	HiveCtx(ctx context.Context, statement string, cb func(*hive.ExecuteResult)) error
	CancelAll(err error)
}

// ProcessFunc ...
type ProcessFunc func(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error)

type DspCampaignProcessFunc func(ctx context.Context, campaignStat *AnalyseStat, duration int) (int, error)
type DspCampaignProcessSqlFunc func(campaignStat *AnalyseStat) (sql string)

func exist(value int, slice []int) bool {
	for _, val := range slice {
		if val == value {
			return true
		}
	}
	return false
}
