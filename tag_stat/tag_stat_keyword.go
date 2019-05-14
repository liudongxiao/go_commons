package tag_stat

import (
	"context"
	"dmp_web/go/model"

	"dmp_web/go/commons/db/hive"
)

var MetricKeyword = []*Metric{
	{"keyword", model.MetricSession, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT t2.name,
       count(1) AS cnt
FROM
    (SELECT search_keyword_id,
            session_id
     FROM dna_v4.dna_v4_session_base
     WHERE %v
         AND dna_v4_session_base.ad_sf_aduser_id IN (%[3]v)
         AND dna_v4_session_base.%[4]v IS NOT NULL
         AND cast(dna_v4_session_base.%[4]v AS string) IN
             (SELECT %[4]v
              FROM %[2]v)
     GROUP BY search_keyword_id,
              session_id) AS t1
JOIN dna_v4.info_value AS t2 ON (t1.search_keyword_id = t2.id
                                 AND t2.id IS NOT NULL)
WHERE t2.name != ""
GROUP BY t2.name
	`},
	{"keyword", model.MetricPageviews, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT t2.name,
       count(1) AS cnt
FROM
    (SELECT search_keyword_id
     FROM dna_v4.dna_v4_session_base
     WHERE %v
         AND dna_v4_session_base.ad_sf_aduser_id IN (%[3]v)
         AND dna_v4_session_base.%[4]v IS NOT NULL
         AND cast(dna_v4_session_base.%[4]v AS string) IN
             (SELECT %[4]v
              FROM %[2]v) ) AS t1
JOIN dna_v4.info_value AS t2 ON (t1.search_keyword_id = t2.id
                                 AND t2.id IS NOT NULL)
WHERE t2.name != ""
GROUP BY t2.name
	`},

	{"keyword", model.MetricVisitors, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT t2.name,
       sum(t1.cnt) AS cnt
FROM
    (SELECT search_keyword_id,
            count(1) AS cnt
     FROM dna_v4.dna_v4_session_base
     WHERE %v
         AND search_keyword_id > 0
         AND dna_v4_session_base.ad_sf_aduser_id IN (%[3]v)
         AND dna_v4_session_base.%[4]v IS NOT NULL
         AND cast(dna_v4_session_base.%[4]v AS string) IN
             (SELECT %[4]v
              FROM %[2]v
              GROUP BY %[4]v)
     GROUP BY search_keyword_id) AS t1
JOIN dna_v4.info_value AS t2 ON (t1.search_keyword_id = t2.id
                                 AND t2.id IS NOT NULL)
WHERE t2.name != ""
GROUP BY t2.name
`},
}

type TagStatKeyword struct {
	*Dimension
}

func (t *TagStatKeyword) GetFunc(metric string) ProcessFunc {
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

func (t *TagStatKeyword) GetModel() model.StatModel {
	return &model.StatKeywordModel
}

func (t *TagStatKeyword) Process(ctx context.Context) {
	baseProcess(ctx, t, MetricKeyword, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}
func (t *TagStatKeyword) ProcessSql() ([]string, error) {
	return baseProcessSqls(t, MetricKeyword, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}

func (t *TagStatKeyword) processS(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatKeyword{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricSession)
		ret.Scan(&s.Name, &s.Sessions)
	})
}

func (t *TagStatKeyword) processP(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatKeyword{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricPageviews)
		ret.Scan(&s.Name, &s.Pageviews)
	})
}

func (t *TagStatKeyword) processV(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatKeyword{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricVisitors)
		ret.Scan(&s.Name, &s.Visitors)
	})
}
