package tag_stat

import (
	"context"
	"dmp_web/go/commons/db/hive"
	"dmp_web/go/commons/log"
	"fmt"
	"strings"
	"time"

	"dmp_web/go/commons/env"
	"dmp_web/go/commons/errors"
	"dmp_web/go/model"
)

//const (
//  dspCampaignTableName = iota
//  pcBaiduTableWithDB
//  mobileBaiduTableWithDB
//  pcBaiduJoinTable
//  mobileBaiduTJoinable
//  pcDistinctbaiduJoinBaiduTable
//  mobileDistinctJoinBaiduTable
//)

const (
	imp = iota
	click
)

var HCli *hive.Client

type AnalyseStat struct {
	cv      model.SqlValue
	analyse model.AnalyseModel

	//多个campaignId 的字符串形式
	campaignIdsInSql   string
	campaignIdsInTable string
	TagId              int64
	Tstage             string

	FromDate string
	ToDate   string

	//table name
	user  string
	imp   string
	click string

	coustom int

	//用于调试查看执行的sql 是否正确
	sqlMetrics  [][]model.SqlMetricModel
	preJoinSqls []model.SqlTable

	hiveContext *hive.HiveContext
}

// NewCampaignStat campaignStat 构造函数, coustom 自定义数据产生的报表
func NewCampStat(coustom int, campaignItem *model.CampaignItem) *AnalyseStat {
	var c *AnalyseStat
	c = &AnalyseStat{
		coustom: coustom,
		analyse: model.NewDspCampaign(campaignItem),
	}

	return c
}

func NewAnalyseStatWithTag(coustom int, Id int64, campaignItem *model.CampaignItem) (a *AnalyseStat, err error) {
	if Id != 0 {
		tag := new(model.Tag)
		ts := model.NewTagStage(Id)
		table, e := ts.GetTbl()
		if err != nil {
			return nil, e

		}
		if err = model.TagModel.FindId(Id, tag); err != nil {
			return
		}
		a = &AnalyseStat{
			analyse: tag,
			Tstage:  table,
			TagId:   Id,
		}

		date, err := tag.GetDate()
		if err != nil {
			return nil, err
		}
		begin, end := date.RangeString()
		dateStr := strings.Join([]string{begin, end}, ",")
		campaignItem = model.NewCampaignItem(tag.GetIds(), tODspType(tag.TypeId), dateStr)
	} else {
		a = NewCampStat(coustom, campaignItem)
	}
	a.hiveContext = hive.NewContext()
	a.setDay(campaignItem)

	a.chooseAnalyse(campaignItem)
	return
}

func tODspType(etype int) string {
	if etype == PC {
		return "pc"
	} else if etype == Mob {
		return "mobile"
	} else {
		panic("not valid type")
	}

}

func (a *AnalyseStat) chooseAnalyse(item *model.CampaignItem) (err error) {
	a.campaignIdsInSql, a.campaignIdsInTable = a.analyse.GetFormat()
	a.setTablesName()
	etype, err := a.analyse.GetType(item.Type)
	if err != nil {
		return errors.ErrType{item.Type}
	}
	a.cv = a.analyse.NewSqlValue(a.coustom, etype)
	TCampaigns := []string{a.user, a.imp, a.click}
	a.preJoinSqls = a.cv.PrejoinSql(a.campaignIdsInSql, a.ToDate, a.FromDate, a.campaignIdsInTable, a.Tstage, TCampaigns)

	var joinTables []string
	for _, v := range a.preJoinSqls {
		joinTables = append(joinTables, v.T)
	}
	a.sqlMetrics = a.cv.ToSqls(a.campaignIdsInSql, a.ToDate, a.FromDate, joinTables)
	return

}

func (a *AnalyseStat) processRate(rate int32) error {
	tag, ok := a.analyse.(*model.Tag)
	if ok {
		tag.StateProcessRate = rate
		return tag.UpdateProgress()

	}
	return nil

}

func (a *AnalyseStat) processCount(ctx context.Context) error {
	tag, ok := a.analyse.(*model.Tag)
	if ok {
		sql := "SELECT count(1) as cnt FROM " + a.Tstage
		err := a.HiveCtx(ctx, sql, func(ret *hive.ExecuteResult) {
			var count int64
			if ret.Next() {
				ret.Scan(&count)
			}
			tag.UpdateCount(count)
		})
		if err != nil {
			log.Errorf("Error HQL:%s, error is %v ", sql, err)
			return err
		}
	}
	return nil
}

//异步执行可以快速测试sql 的正确性
func (a *AnalyseStat) HiveCtx(ctx context.Context, statement string, f func(*hive.ExecuteResult)) error {
	return HCli.AddAsyncCtx(ctx, a.hiveContext, statement, f)
}

func (a *AnalyseStat) String() string {
	return fmt.Sprintf("AnalyseStat: %s, ToDate: %s, FromDate : %s", a.campaignIdsInSql, a.ToDate, a.FromDate)
}

func (a *AnalyseStat) setDay(campaignItem *model.CampaignItem) error {
	if len(campaignItem.Date) == 0 {
		now := time.Now()

		a.ToDate = now.Add(-24 * time.Hour).Format(env.DayFormat)
	} else {
		from, to, _, err := getDayRange(campaignItem.Date)
		if err != nil {
			return err
		}
		a.ToDate = to
		a.FromDate = from
	}
	return nil
}

//ProcessDspCampaign 处理DspCampaignStat 任务
func (a *AnalyseStat) ProcessDspCampaign(ctx context.Context) error {
	if err := a.analyse.Save(); err != nil {
		return err
	}

	if err := a.analyse.MarkStart(); err != nil {
		log.Warn(err)
	}

	if err := a.process(ctx); err != nil {
		if e := a.analyse.MarkError(err); e != nil {
			log.Warn(e)
		}
		return err
	} else {
		if err := a.analyse.MarkFinish(); err != nil {
			log.Warn(err)
		}
	}

	return nil

}

// 左开右闭 时间
func getDayRange(date string) (from, to string, numDay int, err error) {
	dayrange := strings.Split(date, ",")
	if len(dayrange) == 1 {
		_, err := time.Parse(env.DayFormat, dayrange[0])
		if err != nil {
			return "", "", 0, err
		}
		return dayrange[0], dayrange[0], 1, nil

	}
	if len(dayrange) != 2 {
		return "", "", 0, errors.Wrap(errors.ErrDay{date}, "")
	}
	from = dayrange[0]
	to = dayrange[1]
	toTime, err := time.Parse(env.DayFormat, to)
	if err != nil {
		return "", "", 0, err
	}
	fromTime, err := time.Parse(env.DayFormat, from)
	if err != nil {
		return "", "", 0, err
	}
	numDay = (int(toTime.Sub(fromTime).Hours() / 24)) + 1
	if numDay <= 0 {
		err = errors.Wrap(errors.ErrDay{date}, "")
	}
	return
}

func (a *AnalyseStat) setTablesName() {
	campaignIdsInTable := a.campaignIdsInTable
	// 访客表名
	a.user = fmt.Sprintf("%s.%s_user_%s_%s_%s",
		env.HiveDatabase, a.analyse.GetDB(), campaignIdsInTable, a.ToDate, a.FromDate)

	// 展示表表名
	a.imp = fmt.Sprintf("%s.%s_impression_%s_%s_%s",
		env.HiveDatabase, a.analyse.GetDB(), campaignIdsInTable, a.ToDate, a.FromDate)

	// 点击表表名
	a.click = fmt.Sprintf("%s.%s_click_%s_%s_%s",
		env.HiveDatabase, a.analyse.GetDB(), campaignIdsInTable, a.ToDate, a.FromDate)
	return
}

func (a *AnalyseStat) Save(ctx context.Context, sqls [][]model.SqlMetricModel) error {
	var count int
	var all int
	for _, sql := range sqls {
		all += len(sql)
	}
	hiveRetChan := make(chan struct{}, all)
	for i := range sqls {
		sql := sqls[i]
		for j := range sql {
			s := sql[j]
			err := a.HiveCtx(ctx, s.Sql, func(ret *hive.ExecuteResult) {
				// 根据 mongo selector 的条件清理统计数据
				selector := mongoSelector(a, s.Metric)
				err := s.Model.RemoveAll(selector)
				if err != nil {
					a.hiveContext.Error(err)
					return
				}

				var insertRows []interface{}
				for ret.NextPage() {
					for ret.NextInPage() {
						var row *model.StatCommon
						switch a.analyse.(type) {
						case *model.Tag:
							row = &model.StatCommon{
								Metric: s.Metric,
							}
							ret.Scan(&row.Date, &row.AduserId, &row.Name, &row.Value)
						case *model.DspCampaign:
							row = &model.StatCommon{
								Metric: s.Metric,
								TagId:  a.TagId,
							}
							ret.Scan(&row.Date, &row.CampaignId, &row.Name, &row.Value)
						}
						insertRows = append(insertRows, &row)
					}
					if err := ret.Err(); err != nil {
						a.hiveContext.Error(err)
						return
					}
					if err := s.Model.Insert(insertRows); err != nil {
						log.Errorf("%v insert to mongo failed, %v", s.Model, err)
					}
					count += len(insertRows)
					insertRows = insertRows[:0]

				}
				hiveRetChan <- struct{}{}
				log.Debugf("%d.%d sql done", i, j)
			})
			if err != nil {
				log.Errorf("Error HQL:%s, error is %v ", s.Sql, err)
				return err
			}

		}

	}
	for i := 0; i < all; i++ {
		<-hiveRetChan
	}
	log.Debugf("total insert %d rows", count)
	return nil

}

func (a *AnalyseStat) process(ctx context.Context) error {
	//重跑不删除表，如果数据错误，手动删除
	//if err := a.DropTables(ctx); err != nil {
	//      return err
	//}

	a.GetProcessRate()

	if err := a.processCount(ctx); err != nil {
		return err
	}
	hiveRetChan := make(chan struct{}, len(a.preJoinSqls))
	for _, v := range a.preJoinSqls {
		err := a.HiveCtx(ctx, v.Sql, func(ret *hive.ExecuteResult) {
			hiveRetChan <- struct{}{}
		})
		if err != nil {
			log.Errorf("Error HQL:%s, error is %v ", v, err)
			return err
		}
	}
	for i := 0; i < len(a.preJoinSqls); i++ {
		<-hiveRetChan
	}
	log.Debugf("Start analyse  task, user table : %s, impression table : %s, click table : %s", a.user, a.imp, a.click)
	return a.Save(ctx, a.sqlMetrics)
}

func (a *AnalyseStat) GetProcessRate() {
	go func() {
		for {
			<-time.Tick(time.Duration(time.Duration(TaskProcessFrequency) * time.Second))
			total, undone, done := a.hiveContext.Get()
			if undone != 0 {
				if total > 0 {
					rate := int32(float32(done) / float32(total) * 100)
					a.processRate(rate)
				}
			} else {
				a.processRate(100)
				return
			}
		}
	}()
}
