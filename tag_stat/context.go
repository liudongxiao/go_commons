package tag_stat

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	//"dmp_web/go/commons/log"

	"bytes"
	"dmp_web/go/commons/db/hive"
	"dmp_web/go/model"
	"dmp_web/go/model/model_rule"
)

type Dimension struct {
	hiveContext *hive.HiveContext
	hive        hive.Cli
	tag         *model.Tag
	now         string
	stage       *model.TagStage
	date        *model_rule.Date
	suppliers   map[model.Product]string // 例： {DNA => "111,222,333", DSP => "111"}
}

func NewDimension(hcli hive.Cli, tag *model.Tag, stage *model.TagStage) (*Dimension, error) {
	date, err := tag.GetDate()
	if err != nil {
		return nil, err
	}
	suppliers, err := model.SupplierModel.ToProducts(tag.SupplierIds)
	if err != nil {
		return nil, err
	}
	// map[model.Product][]int64  => map[model.Product]string
	convertor := func(in map[model.Product][]int) map[model.Product]string {
		out := make(map[model.Product]string, len(in))
		for product, suppliers := range in {
			suppliersStrs := make([]string, 0, len(suppliers))
			for _, supplier := range suppliers {
				suppliersStrs = append(suppliersStrs, strconv.Itoa(supplier))
			}
			out[product] = strings.Join(suppliersStrs, ",")
		}
		return out
	}
	return &Dimension{
		hive:        hcli,
		hiveContext: hive.NewContext(),
		now:         time.Now().Format("20060102"),
		tag:         tag,
		stage:       stage,
		date:        date,
		suppliers:   convertor(suppliers),
	}, nil
}

func (d *Dimension) GetTag() *model.Tag {
	return d.tag
}

func (d *Dimension) TagId() int64 {
	return d.tag.Id
}

func (d *Dimension) Clean(c model.Cleaner, metric, dt string) error {
	return c.Clean(metric, d.TagId(), dt)
}

// suppliersGroup： []string{"111,222,333", "444,555", "666,777,888"}, 分别使用layout中的 3、4、5号占位符
func (d *Dimension) StatementByRange(layout string, field string, date *model_rule.Date, suppliers []string, name string) (string, error) {
	args := append([]interface{}{}, date.ToHQL(field), d.stage.Tbl[TagStageTable])
	suppliersGroupStr := strings.Join(suppliers, ",")
	etype := d.tag.TypeId
	if etype == 1 {
		args = append(args, suppliersGroupStr, "visitor_id", "visitorid")
		return fmt.Sprintf(layout, args...), nil
	} else if etype == 2 {
		args = append(args, suppliersGroupStr, "did", "did")
		return fmt.Sprintf(layout, args...), nil
	} else {
		return "", fmt.Errorf("not vaild type %d", etype)
	}
}

// suppliersGroup： []string{"111,222,333", "444,555", "666,777,888"}, 分别使用layout中的 3、4、5号占位符
func (d *Dimension) StatementsByDay(layout string, field string, dts []string,
	suppliersGroup []string, name string) ([]string, error) {
	if len(dts) == 0 {
		return []string{
			fmt.Sprintf(layout, "!@#$%", d.stage.Tbl[TagStageTable])}, nil
	}
	ret := make([]string, len(dts))
	args := append([]interface{}{}, "date holder", d.stage.Tbl[TagStageTable])
	// args[0] "date holder" 只是占位, 下面将替换成实际的日期
	suppliersGroupStr := strings.Join(suppliersGroup, ",")
	etype := d.tag.TypeId
	if etype == 1 {
		args = append(args, suppliersGroupStr, "visitor_id", "visitorid")
	} else if etype == 2 {
		args = append(args, suppliersGroupStr, "did", "did")
	} else {
		return nil, fmt.Errorf("not vaild type %d", etype)
	}
	for idx, d := range dts {
		args[0] = field + "=" + d // 将第一个参数替换成具体日期条件语句
		ret[idx] = fmt.Sprintf(layout, args...)
	}
	return ret, nil
}

func (d *Dimension) StatementByRangeWithBaiduTable(layout string, field string, date *model_rule.Date,
	suppliersGroup []string, name string) (string, error) {
	var args []interface{}
	if name == model.MetricValue {
		args = append([]interface{}{}, date.ToHQL(field), d.stage.Tbl[DistinctJoinBiaudTable])
	} else {
		args = append([]interface{}{}, date.ToHQL(field), d.stage.Tbl[JoinBaiduTable])
	}
	suppliersGroupStr := strings.Join(suppliersGroup, ",")
	etype := d.tag.TypeId
	if etype == 1 {
		args = append(args, suppliersGroupStr, "visitor_id", "visitorid")

		// args[0] dt 时间范围
		// args[1] visitorid table name
		// args[2] 是供应商 Id
		return fmt.Sprintf(layout, args...), nil
	} else if etype == 2 {
		args = append(args, suppliersGroupStr, "did", "did")
		return fmt.Sprintf(layout, args...), nil
	} else {
		return "", fmt.Errorf("not vaild type %d", etype)
	}
}

func (d *Dimension) HiveCtx(ctx context.Context, statement string, f func(*hive.ExecuteResult)) error {
	return d.hive.AddAsyncCtx(ctx, d.hiveContext, statement, f)
}

func (d *Dimension) CancelAll(err error) {
	d.hiveContext.Error(err)
}

func (d *Dimension) Progress() string {
	return d.hiveContext.Progress()
}

func (d *Dimension) GetDate() *model_rule.Date {
	return d.date
}

func (d *Dimension) baseProcess(ctx context.Context, p MetricProcessor, ret *hive.ExecuteResult, f func(*[]interface{})) (int, error) {
	size := 0
	var data []interface{}
	for ret.NextPage() {
		for ret.NextInPage() {
			f(&data)
		}

		size += len(data)
		if len(data) > 0 {
			if err := p.GetModel().Insert(data); err != nil {
				return -1, err
			}
		}
		data = data[:0]
		//if err := env.CtxToNotify(ctx, env.RedisDoneKey); err != nil {
		//	log.Error(err)
		//}
	}
	return size, nil
}

func suppilerGroupValueToStr(supplierGroup map[model.Product]string, separator string) string {
	buf := bytes.NewBuffer(nil)
	var i int
	for _, suppiler := range supplierGroup {
		buf.WriteString(suppiler)
		if i == len(supplierGroup)-1 {
			break
		}
		buf.WriteString(separator)
		i++
	}
	return buf.String()
}
