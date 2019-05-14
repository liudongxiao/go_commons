package tag_stat

import (
	"context"
	"dmp_web/go/commons/log"

	"dmp_web/go/commons/db/hive"
	"dmp_web/go/model"
	"dmp_web/go/model/model_rule"
)

var MetricIndustry = []*Metric{
	{"industry", model.MetricValue, "dt", model.NoneProduct, []int{PC, Mob},
		`
SELECT ds.industry,
       count(1) AS cont
FROM %[2]v AS ds
GROUP BY ds.industry
		`},

	// 	{model.MetricTotal, "dt", model.NoneProduct,
	// 		`
	// select count(1) as cnt
	// from
	// (
	// 	select dubc.userid
	// 	from dmp.user_baidu_category as dubc
	// 	join %[2]v as t1
	// 	on (t1.visitor_id = dubc.userid
	// and ds.visitor_id is not null) 	group by dubc.userid
	// ) as t1
	// 	`},
	{"industry", model.MetricImpressions, "dt", []model.Product{model.ProductDSP}, []int{PC, Mob},
		`
SELECT ds.industry,
       count(1)
FROM %[2]v AS ds
INNER JOIN
    (SELECT %[5]v AS %[4]v
     FROM dsp.dw_ana_logs
     WHERE %[1]v
         AND aduserid IN (%[3]v) ) AS ca ON (ca.%[4]v = ds.%[4]v)
GROUP BY ds.industry
		`},
	{"industry", model.MetricClick, "dt", []model.Product{model.ProductDSP}, []int{PC, Mob},
		`
SELECT ds.industry,
       count(1)
FROM %[2]v AS ds
INNER JOIN
    (SELECT %[5]v AS %[4]v
     FROM dsp.dw_whisky_logs
     WHERE %[1]v
         AND aduserid IN (%[3]v) ) AS ck ON (ck.%[4]v = ds.%[4]v)
GROUP BY ds.industry
		`},
}

type TagStatIndustry struct {
	*Dimension
}

func (t *TagStatIndustry) GetFunc(metric string) ProcessFunc {
	switch metric {
	case model.MetricValue:
		return t.processV
	case model.MetricTotal:
		return t.processT
	case model.MetricImpressions:
		return t.processImp
	case model.MetricClick:
		return t.processClk
	}
	return nil
}

func (t *TagStatIndustry) GetModel() model.StatModel {
	return &model.StatIndustryModel
}

func (t *TagStatIndustry) Process(ctx context.Context) {
	baseProcess(ctx, t, MetricIndustry, &ProcessConfig{SuppliersStrs: t.Dimension.suppliers})
}
func (t *TagStatIndustry) ProcessSql() ([]string, error) {
	return baseProcessSqls(t, MetricIndustry, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}

func (t *TagStatIndustry) processV(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatIndustry{
		TagId: t.TagId(),
		Date:  dt,
	}
	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricValue)
		ret.Scan(&s.Name, &s.Value)
	})
}

func (t *TagStatIndustry) processT(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatIndustry{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricTotal)
		ret.Scan(&s.Total)
	})
}

func (t *TagStatIndustry) processImp(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	t.GetModel().Clean(model.MetricImpressions, t.TagId(), "")
	size := 0
	var insertRows []interface{}

	for ret.NextPage() {
		for ret.NextInPage() {
			row := model.StatSex{
				TagId:  t.TagId(),
				Metric: model.MetricImpressions,
				Date:   dt,
			}

			ret.Scan(&row.Name, &row.Value)
			insertRows = append(insertRows, &row)
			size += 1
		}

		if err := t.GetModel().Insert(insertRows); err != nil {
			log.Error("", err.Error())
		}

		insertRows = insertRows[:0]
	}

	return size, nil
}

func (t *TagStatIndustry) processClk(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	t.GetModel().Clean(model.MetricClick, t.TagId(), "")
	size := 0
	var insertRows []interface{}

	for ret.NextPage() {
		for ret.NextInPage() {
			row := model.StatSex{
				TagId:  t.TagId(),
				Metric: model.MetricClick,
				Date:   dt,
			}

			ret.Scan(&row.Name, &row.Value)
			insertRows = append(insertRows, &row)
			size += 1
		}

		if err := t.GetModel().Insert(insertRows); err != nil {
			log.Error("", err.Error())
		}

		insertRows = insertRows[:0]
	}

	return size, nil
}

// suppliersGroup： []string{"111,222,333", "444,555", "666,777,888"}, 分别使用layout中的 3、4、5号占位符
func (t *TagStatIndustry) StatementByRange(layout string, field string, date *model_rule.Date,
	suppliersGroup []string, name string) (string, error) {
	return t.StatementByRangeWithBaiduTable(layout, field, date, suppliersGroup, name)
}
