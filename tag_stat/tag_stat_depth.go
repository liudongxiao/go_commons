package tag_stat

import (
	"context"
	"dmp_web/go/model"
	"fmt"

	"dmp_web/go/commons/db/hive"
)

var MetricDepth = []*Metric{
	{
		"depth", model.MetricSession, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT t1.depth,
       t1.cnt,
       (t1.depth*t1.cnt) AS pageviews
FROM
    (SELECT pageviews AS depth,
            count(1) AS cnt
     FROM dna_v4.dna_v4_session_base
     WHERE %[1]v
         AND cast(dna_v4_session_base.%[4]v AS string) IN
             (SELECT %[4]v
              FROM %[2]v)
         AND dna_v4_session_base.ad_sf_aduser_id IN (%[3]v)
         AND dna_v4_session_base.%[4]v IS NOT NULL
     GROUP BY pageviews) AS t1
     `,
	},
	{
		"depth", model.MetricVisitors, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT t1.pageviews AS depth,
       count(1) AS cnt
FROM
    (SELECT max(pageviews) AS pageviews
     FROM dna_v4.dna_v4_session_base
     WHERE %[1]v
         AND cast(dna_v4_session_base.%[4]v AS string) IN
             (SELECT %[4]v
              FROM %[2]v group by %[4]v)
         AND dna_v4_session_base.ad_sf_aduser_id IN (%[3]v)
         AND dna_v4_session_base.%[4]v IS NOT NULL
     GROUP BY %[4]v) AS t1
GROUP BY t1.pageviews
`,
	},
}

type TagStatDepth struct {
	*Dimension
}

func (t *TagStatDepth) GetFunc(metric string) ProcessFunc {
	switch metric {
	case model.MetricPageviews, model.MetricSession:
		return t.processSP
	case model.MetricVisitors:
		return t.processV
	}
	return nil
}

func (t *TagStatDepth) GetModel() model.StatModel {
	return &model.StatDepthModel
}

func (t *TagStatDepth) Process(ctx context.Context) {
	baseProcess(ctx, t, MetricDepth, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}
func (t *TagStatDepth) ProcessSql() ([]string, error) {
	return baseProcessSqls(t, MetricDepth, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}

func (t *TagStatDepth) processSP(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	t.GetModel().Clean(model.MetricPageviews, t.TagId(), dt)

	obj := model.StatDepth{
		TagId: t.TagId(),
		Date:  dt,
	}
	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		var depth int64
		var sessions int64
		var pageviews int64
		ret.Scan(&depth, &sessions, &pageviews)
		name := fmt.Sprint(depth)
		s := obj.New(data)
		s.SetSessions(name, sessions)
		s = obj.New(data)
		s.SetPageviews(name, pageviews)
	})
}

func (t *TagStatDepth) processV(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := model.StatDepth{
		TagId: t.TagId(),
		Date:  dt,
	}
	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		var depth int64
		var cnt int64
		ret.Scan(&depth, &cnt)
		s := obj.New(data)
		s.SetVisitors(fmt.Sprint(depth), cnt)
	})
}
