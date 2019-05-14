package tag_stat

import (
	"context"
	"dmp_web/go/model"

	"dmp_web/go/commons/db/hive"
	"dmp_web/go/commons/log"
)

var MetricAdSummary = []*Metric{
	{
		Name:                   model.MetricClick,
		DateField:              "dt",
		SupplierDependProducts: []model.Product{model.ProductDSP},
		etype:                  []int{PC, Mob},
		Layout: `SELECT clk.dt, clk.spot_id, ad.name, clk.cnt
FROM (SELECT dt AS dt, spotid AS spot_id, count(1) AS cnt
    FROM dsp.dw_whisky_logs
    WHERE
        %[1]v AND
        dw_whisky_logs.aduserid IN (%[3]v) AND
        dw_whisky_logs.%[5]v IN (SELECT %[4]v FROM %[2]v)
    GROUP BY dt, spotid) AS clk
LEFT JOIN dsp.meta_ad_position AS ad
ON clk.spot_id = ad.id`,
	},
	{
		Name:                   model.MetricImpressions,
		DateField:              "dt",
		SupplierDependProducts: []model.Product{model.ProductDSP},
		etype:                  []int{PC, Mob},
		Layout: `SELECT imp.dt, imp.spot_id, ad.name, imp.cnt
FROM (SELECT dt AS dt, spotid AS spot_id, count(1) AS cnt
    FROM dsp.dw_ana_logs
    WHERE
        %[1]v AND
        dw_ana_logs.aduserid IN (%[3]v) AND
        dw_ana_logs.%[5]v IN (SELECT %[4]v FROM %[2]v)
    GROUP BY dt, spotid) AS imp
LEFT JOIN dsp.meta_ad_position AS ad
ON imp.spot_id = ad.id`,
	},
}

type TagStatAdSummary struct {
	*Dimension
}

func (t *TagStatAdSummary) GetFunc(metric string) ProcessFunc {
	switch metric {
	case model.MetricValue:
		return t.processV
	case model.MetricImpressions:
		return t.processImpression
	case model.MetricClick:
		return t.processClick
	}

	return nil
}

func (t *TagStatAdSummary) GetModel() model.StatModel {
	return &model.StatAdSummaryModel
}

func (t *TagStatAdSummary) Process(ctx context.Context) {
	baseProcess(ctx, t, MetricAdSummary, &ProcessConfig{SuppliersStrs: t.Dimension.suppliers})
}
func (t *TagStatAdSummary) ProcessSql() ([]string, error) {
	return baseProcessSqls(t, MetricAdSummary, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}

func (t *TagStatAdSummary) processClick(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	t.GetModel().Clean(model.MetricClick, t.TagId(), "")
	size := 0
	var insertRows []interface{}

	for ret.NextPage() {
		for ret.NextInPage() {
			row := model.StatAdSummary{
				TagId:  t.TagId(),
				Metric: model.MetricClick,
			}

			ret.Scan(&row.DateTime, &row.SpotId, &row.Name, &row.Value)

			insertRows = append(insertRows, row)
			size += 1
		}

		t.GetModel().Insert(insertRows)
		insertRows = insertRows[:0]

	}

	return size, nil
}

func (t *TagStatAdSummary) processImpression(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	t.GetModel().Clean(model.MetricImpressions, t.TagId(), "")
	size := 0
	var insertRows []interface{}

	for ret.NextPage() {
		for ret.NextInPage() {
			row := model.StatAdSummary{
				TagId:  t.TagId(),
				Metric: model.MetricImpressions,
			}

			ret.Scan(&row.DateTime, &row.SpotId, &row.Name, &row.Value)
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

func (t *TagStatAdSummary) processV(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatAdSummary{
		TagId: t.TagId(),
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricValue)
		ret.Scan(&s.DateTime, &s.SpotId, &s.Name, &s.Value)
	})
}
