package tag_stat

import (
	"context"
	"dmp_web/go/model"

	"dmp_web/go/commons/db/hive"
)

var MetricSource = []*Metric{
	{"source", model.MetricSession, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`

SELECT t2.name,
       count(1) AS cnt
FROM
    (SELECT referer_domain_id
     FROM dna_v4.dna_v4_session_base
     WHERE %[1]v
         AND cast(dna_v4_session_base.%[4]v AS string) IN
             (SELECT %[4]v
              FROM %[2]v)
         AND dna_v4_session_base.ad_sf_aduser_id IN (%[3]v)
         AND dna_v4_session_base.%[4]v IS NOT NULL ) AS t1
JOIN dna_v4.info_gg_source AS t2 ON (t1.referer_domain_id = t2.id and t2.id IS NOT NULL)
GROUP BY t2.name
`},
	{"source", model.MetricPageviews, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT t2.name,
       sum(pageviews) AS cnt
FROM
    (SELECT referer_domain_id,
            pageviews
     FROM dna_v4.dna_v4_session_base
     WHERE %[1]v
         AND cast(dna_v4_session_base.%[4]v AS string) IN
             (SELECT %[4]v
              FROM %[2]v)
         AND dna_v4_session_base.ad_sf_aduser_id IN (%[3]v)
         AND dna_v4_session_base.%[4]v IS NOT NULL ) AS t1
JOIN dna_v4.info_gg_source AS t2 ON (t1.referer_domain_id = t2.id and t2.id IS NOT NULL)
GROUP BY t2.name
	`},
	{"source", model.MetricVisitors, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT t2.name,
       count(1) AS cnt
FROM
    (SELECT referer_domain_id
     FROM dna_v4.dna_v4_session_base
     WHERE %[1]v
         AND cast(dna_v4_session_base.%[4]v AS string) IN
             (SELECT %[4]v
              FROM %[2]v
              GROUP BY %[4]v)
         AND dna_v4_session_base.ad_sf_aduser_id IN (%[3]v)
         AND dna_v4_session_base.%[4]v IS NOT NULL
     GROUP BY referer_domain_id,
              %[4]v) AS t1
JOIN dna_v4.info_gg_source AS t2 ON (t1.referer_domain_id = t2.id
                                     AND t2.id IS NOT NULL)
GROUP BY t2.name
	`},
}

type TagStatSource struct {
	*Dimension
}

func (t *TagStatSource) GetFunc(metric string) ProcessFunc {
	switch metric {
	case model.MetricSession:
		return t.processS
	case model.MetricPageviews:
		return t.processP
	case model.MetricVisitors:
		return t.processV
	}
	return nil
}

func (t *TagStatSource) GetModel() model.StatModel {
	return &model.StatSourceModel
}

func (t *TagStatSource) Process(ctx context.Context) {
	baseProcess(ctx, t, MetricSource, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}
func (t *TagStatSource) ProcessSql() ([]string, error) {
	return baseProcessSqls(t, MetricSource, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}

func (t *TagStatSource) processS(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatSource{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricSession)
		ret.Scan(&s.Name, &s.Sessions)
	})
}

func (t *TagStatSource) processP(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatSource{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricPageviews)
		ret.Scan(&s.Name, &s.Pageviews)
	})
}

func (t *TagStatSource) processV(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatSource{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricVisitors)
		ret.Scan(&s.Name, &s.Visitors)
	})
}
