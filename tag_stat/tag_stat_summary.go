package tag_stat

import (
	"context"
	"dmp_web/go/model"

	"dmp_web/go/commons/db/hive"
)

var MetricSummary = []*Metric{
	{"summary", model.MetricSession, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT count(1) AS cnt,
       sum(pageviews)
FROM dna_v4.dna_v4_session_base
WHERE %[1]v
    AND cast(dna_v4_session_base.%[4]v AS string) IN
        (SELECT %[4]v
         FROM %[2]v)
    AND dna_v4_session_base.ad_sf_aduser_id IN (%[3]v)
    AND %[4]v IS NOT NULL
	`},
	{"summary", model.MetricVisitors, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT count(1) AS cnt
FROM
    (SELECT %[4]v
     FROM dna_v4.dna_v4_session_base
     WHERE %[1]v
         AND cast(dna_v4_session_base.%[4]v AS string) IN
             (SELECT %[4]v
              FROM %[2]v
              GROUP BY %[4]v)
         AND dna_v4_session_base.ad_sf_aduser_id IN (%[3]v)
         AND %[4]v IS NOT NULL
     GROUP BY %[4]v) AS t1
`},

	{"summary", model.MetricImpressions, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT count(1) AS cnt
FROM dsp.dw_ana_logs
WHERE %[1]v
    AND dw_ana_logs.aduserid IN (%[3]v)
    AND visitorid IS NOT NULL
    AND dw_ana_logs.visitorid IN
        (SELECT %[4]v
         FROM %[2]v)
         	`},
	{"summary", model.MetricClick, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT count(1) AS cnt
FROM dsp.dw_whisky_logs
WHERE %[1]v
    AND dw_whisky_logs.visitorid IN
        (SELECT %[4]v
         FROM %[2]v)
    AND dw_whisky_logs.aduserid IN (%[3]v)
    AND visitorid IS NOT NULL
    	`},
}

type TagStatSummary struct {
	*Dimension
}

func (t *TagStatSummary) GetFunc(metric string) ProcessFunc {
	switch metric {
	case model.MetricSession:
		return t.processSP
	case model.MetricImpressions:
		return t.processI
	case model.MetricVisitors:
		return t.processV
	case model.MetricClick:
		return t.processC
	}
	return nil
}

func (t *TagStatSummary) GetModel() model.StatModel {
	return &model.StatSummaryModel
}

func (t *TagStatSummary) Process(ctx context.Context) {
	dts := t.Dimension.date.DayByDay()
	if len(dts) > 7 {
		dts = dts[len(dts)-7:]
	}
	baseProcess(ctx, t, MetricSummary, &ProcessConfig{Days: dts, SuppliersStrs: t.Dimension.suppliers})
}
func (t *TagStatSummary) ProcessSql() ([]string, error) {
	return baseProcessSqls(t, MetricSummary, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}

func (t *TagStatSummary) processSP(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	t.GetModel().Clean(model.MetricPageviews, t.tag.Id, dt)

	obj := &model.StatSummary{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		var session int64
		var pageviews int64
		ret.Scan(&session, &pageviews)
		s := obj.New(data, model.MetricSession)
		s.Sessions = session
		s = obj.New(data, model.MetricPageviews)
		s.Pageviews = pageviews
	})
}

func (t *TagStatSummary) processI(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatSummary{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricImpressions)
		ret.Scan(&s.Impressions)
	})
}

func (t *TagStatSummary) processV(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatSummary{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricVisitors)
		ret.Scan(&s.Visitors)
	})
}

func (t *TagStatSummary) processC(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatSummary{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricClick)
		ret.Scan(&s.Clicks)
	})
}
