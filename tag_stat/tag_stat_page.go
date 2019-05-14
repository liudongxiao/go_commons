package tag_stat

import (
	"context"
	"dmp_web/go/model"

	"dmp_web/go/commons/db/hive"
)

var MetricPage = []*Metric{
	{"page", model.MetricSession, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT t2.name,
       t1.cnt
FROM
    (SELECT t3.page_url_id,
            count(1) AS cnt
     FROM
         (SELECT page_url_id,
                 session_id
          FROM dna_v4.dna_v4_event_flow_base
          WHERE %[1]v
              AND dna_v4_event_flow_base.ad_sf_aduser_id IN (%[3]v)
              AND %[4]v IS NOT NULL
              AND cast(dna_v4_event_flow_base.%[4]v AS string) IN
                  (SELECT %[4]v
                   FROM %[2]v)
          GROUP BY page_url_id,
                   session_id) AS t3
     GROUP BY t3.page_url_id) AS t1
JOIN dna_v4.info_url AS t2 ON (t1.page_url_id = t2.id
                               AND t2.id IS NOT NULL)
WHERE coalesce(t2.name, "") != ""
	`},
	{"page", model.MetricPageviews, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT iu.name,
       it.name,
       count(1) AS cnt
FROM
    (SELECT page_title_id,
            page_url_id
     FROM dna_v4.dna_v4_event_flow_base
     WHERE %[1]v
         AND ad_sf_aduser_id IN (%[3]v)
         AND %[4]v IS NOT NULL
         AND cast(%[4]v AS string) IN
             (SELECT %[4]v
              FROM %[2]v)) AS df
INNER JOIN dna_v4.info_url AS iu ON (df.page_url_id = iu.id
                                     AND coalesce(iu.name, "") != ""
                                     AND df.page_url_id IS NOT NULL)
INNER  JOIN dna_v4.info_title AS it ON (df.page_title_id = it.id
                                        AND coalesce(it.name, "") != ""
                                        AND df.page_url_id IS NOT NULL)
GROUP BY df.page_url_id,
         iu.name,
         it.name
         	`},
	{"page", model.MetricVisitors, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT t2.name,
       t1.cnt
FROM
    (SELECT t3.page_url_id,
            count(1) AS cnt
     FROM
         (SELECT page_url_id,
                 %[4]v
          FROM dna_v4.dna_v4_event_flow_base
          WHERE %[1]v
              AND dna_v4_event_flow_base.ad_sf_aduser_id IN (%[3]v)
              AND dna_v4_event_flow_base.%[4]v IS NOT NULL
              AND cast(dna_v4_event_flow_base.%[4]v AS string) IN
                  (SELECT %[4]v
                   FROM %[2]v
                   GROUP BY %[4]v)
          GROUP BY page_url_id,
                   %[4]v) AS t3
     GROUP BY t3.page_url_id) AS t1
JOIN dna_v4.info_url AS t2 ON (t1.page_url_id = t2.id
                               AND t2.id IS NOT NULL)
WHERE coalesce(t2.name, "") != ""
	`},
}

type TagStatPage struct {
	*Dimension
}

func (t *TagStatPage) GetFunc(metric string) ProcessFunc {
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

func (t *TagStatPage) GetModel() model.StatModel {
	return &model.StatPageModel
}

func (t *TagStatPage) Process(ctx context.Context) {
	baseProcess(ctx, t, MetricPage, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}
func (t *TagStatPage) ProcessSql() ([]string, error) {
	return baseProcessSqls(t, MetricPage, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}

func (t *TagStatPage) processS(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatPage{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricSession)
		ret.Scan(&s.Url, &s.Title, &s.Sessions)
	})
}

func (t *TagStatPage) processP(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatPage{
		TagId: t.TagId(),
		Date:  dt,
	}
	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricPageviews)
		ret.Scan(&s.Url, &s.Title, &s.Pageviews)
	})
}

func (t *TagStatPage) processV(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatPage{
		TagId: t.TagId(),
		Date:  dt,
	}
	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricVisitors)
		ret.Scan(&s.Url, &s.Title, &s.Visitors)
	})
}
