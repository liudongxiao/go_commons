package tag_stat

import (
	"context"
	"dmp_web/go/commons/db/hive"
	"dmp_web/go/model"
	"dmp_web/go/model/model_rule"
)

var MetricSex = []*Metric{
	{
		dimension:              "sex",
		Name:                   model.MetricValue,
		DateField:              "dt",
		SupplierDependProducts: model.NoneProduct,
		etype:                  []int{PC, Mob},
		Layout: `
SELECT ds.sex,
       count(1) AS cnt
FROM %[2]v AS ds
GROUP BY ds.sex
	`,
	},

	{
		dimension:              "sex",
		Name:                   model.MetricImpressions,
		DateField:              "dt",
		SupplierDependProducts: []model.Product{model.ProductDSP},
		etype:                  []int{PC, Mob},
		Layout: `
SELECT ds.sex,
       count(1)
FROM %[2]v AS ds
INNER JOIN
    (SELECT %[5]v AS %[4]v
     FROM dsp.dw_ana_logs
     WHERE %[1]v
         AND aduserid IN (%[3]v) ) AS ca ON (ds.%[4]v = ca.%[4]v)
GROUP BY ds.sex
		`,
	},

	{
		dimension:              "sex",
		Name:                   model.MetricClick,
		DateField:              "dt",
		SupplierDependProducts: []model.Product{model.ProductDSP},
		etype:                  []int{PC, Mob},
		Layout: `
SELECT ds.sex,
       count(1)
FROM %[2]v AS ds
INNER JOIN
    (SELECT %[5]v AS %[4]v
     FROM dsp.dw_whisky_logs
     WHERE %[1]v
         AND aduserid IN (%[3]v) ) AS ck ON (ds.%[4]v = ck.%[4]v)
GROUP BY ds.sex
`},
}

type TagStatSex struct {
	*Dimension
}

func (t *TagStatSex) GetFunc(metric string) ProcessFunc {
	switch metric {
	case model.MetricValue:
		return t.processV
	case model.MetricClick:
		return t.processClk
	case model.MetricImpressions:
		return t.processImp
	}
	return nil
}

func (t *TagStatSex) GetModel() model.StatModel {
	return &model.StatSexModel
}

func (t *TagStatSex) Process(ctx context.Context) {
	baseProcess(ctx, t, MetricSex, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}
func (t *TagStatSex) ProcessSql() ([]string, error) {
	return baseProcessSqls(t, MetricSex, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}

func (t *TagStatSex) processV(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatSex{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricValue)
		ret.Scan(&s.Name, &s.Value)
	})
}

func (t *TagStatSex) processImp(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatSex{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricImpressions)
		ret.Scan(&s.Name, &s.Value)
	})
}

func (t *TagStatSex) processClk(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatSex{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricClick)
		ret.Scan(&s.Name, &s.Value)
	})
}

// suppliersGroup： []string{"111,222,333", "444,555", "666,777,888"}, 分别使用layout中的 3、4、5号占位符
func (t *TagStatSex) StatementByRange(layout string, field string, date *model_rule.Date,
	suppliersGroup []string, name string) (string, error) {
	return t.StatementByRangeWithBaiduTable(layout, field, date, suppliersGroup, name)
}
