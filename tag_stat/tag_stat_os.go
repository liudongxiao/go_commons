package tag_stat

import (
	"context"
	"dmp_web/go/commons/db/hive"
	"dmp_web/go/model"
)

var MetricOs = []*Metric{
	// 	{model.MetricSession, "dt", []model.Product{model.ProductDNA},
	// 		`
	// select coalesce(t3.name, "Unknown") as ostypename, t2.ostype, t2.cnt
	// from
	// (
	// 	select t1.ostype, count(1) as cnt
	// 	from
	// 	(
	// 		select
	// 			os_id as ostype
	// 		from
	// 			dna_v4.dna_v4_session_base
	// 		where
	// 			%[1]v and
	// 			trend_session.ad_sf_aduser_id in (%[3]v) and
	// 			trend_session.visitor_id in (select visitor_id from %[2]v)
	// 		group by session_id,os_id
	// 	) as t1
	// 	group by t1.ostype
	// ) as t2
	// left outer join
	//   dsp.dim_ostype_rv t3
	// on (t2.ostype = t3.id and t3.id is not null)
	// 	`},

	// 	{model.MetricPageviews, "dt", []model.Product{model.ProductDNA},
	// 		`
	// select coalesce(t3.name, "Unknown") as ostypename, t2.ostype, t2.cnt
	// from
	// (
	// 	select t1.ostype, count(1) as cnt
	// 	from
	// 	(
	// 		select
	// 			os_id as ostype
	// 		from
	// 			dna_v4.dna_v4_session_base
	// 		where
	// 			%[1]v and
	// 			trend_session.ad_sf_aduser_id in (%[3]v) and
	// 			trend_session.visitor_id in (select visitor_id from %[2]v)
	// 	) as t1
	// 	group by t1.ostype
	// ) as t2
	// left outer join
	//   dsp.dim_ostype_rv t3
	// on (t2.ostype = t3.id and t3.id is not null)

	// 	`},

	{"os", model.MetricVisitors, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT coalesce(t3.name, "Unknown") AS ostypename,
       t2.ostype,
       t2.cnt
FROM
    (SELECT t1.ostype,
            count(1) AS cnt
     FROM
         (SELECT ostype
          FROM dsp.dw_ana_logs
          WHERE %[1]v
              AND dw_ana_logs.aduserid IN (%[3]v)
              AND dw_ana_logs.visitorid IN
                  (SELECT visitor_id
                   FROM %[2]v)
              AND visitorid IS NOT NULL
          GROUP BY visitorid,
                   ostype
          UNION ALL SELECT os_id AS ostype
          FROM dna_v4.dna_v4_session_base
          WHERE %[1]v
              AND dna_v4_session_base.ad_sf_aduser_id IN (%[3]v)
              AND visitor_id IS NOT NULL
              AND cast(dna_v4_session_base.visitor_id AS string) IN
                  (SELECT visitor_id
                   FROM %[2]v)
              AND dna_v4_session_base.visitor_id IS NOT NULL
          GROUP BY visitor_id,
                   os_id) AS t1
     GROUP BY t1.ostype) AS t2
LEFT OUTER JOIN
    (SELECT id,
            max(name) AS name
     FROM dsp.dim_ostype
     GROUP BY id) AS t3 ON (t2.ostype = t3.id
                            AND t3.id IS NOT NULL)
    `},

	{"os", model.MetricImpressions, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT coalesce(t3.name, "Unknown") AS ostypename,
       t2.ostype,
       t2.cnt
FROM
    (SELECT t1.ostype,
            count(1) AS cnt
     FROM
         (SELECT ostype
          FROM dsp.dw_ana_logs
          WHERE %[1]v
              AND dw_ana_logs.aduserid IN (%[3]v)
              AND visitorid IS NOT NULL
              AND dw_ana_logs.visitorid IN
                  (SELECT visitor_id
                   FROM %[2]v) ) AS t1
     GROUP BY t1.ostype) AS t2
LEFT OUTER JOIN
    (SELECT id,
            max(name) AS name
     FROM dsp.dim_ostype
     GROUP BY id) AS t3 ON (t2.ostype = t3.id
                            AND t3.id IS NOT NULL)
      `},

	{"os", model.MetricClick, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT coalesce(t3.name, "Unknown") AS ostypename,
       t2.ostype,
       t2.cnt
FROM
    (SELECT t1.ostype,
            count(1) AS cnt
     FROM
         (SELECT ostype
          FROM dsp.dw_whisky_logs
          WHERE %[1]v
              AND dw_whisky_logs.aduserid IN (%[3]v)
              AND visitorid IS NOT NULL
              AND dw_whisky_logs.visitorid IN
                  (SELECT visitor_id
                   FROM %[2]v) ) AS t1
     GROUP BY t1.ostype) AS t2
LEFT OUTER JOIN
    (SELECT id,
            max(name) AS name
     FROM dsp.dim_ostype
     GROUP BY id) AS t3 ON (t2.ostype = t3.id
                            AND t3.id IS NOT NULL)
     `},
}

type TagStatOs struct {
	*Dimension
}

func (t *TagStatOs) GetFunc(metric string) ProcessFunc {
	switch metric {
	case model.MetricSession:
		return t.processS
	case model.MetricPageviews:
		return t.processP
	case model.MetricVisitors:
		return t.processV
	case model.MetricClick:
		return t.processC
	case model.MetricImpressions:
		return t.processI
	}
	return nil
}

func (t *TagStatOs) GetModel() model.StatModel {
	return &model.StatOsModel
}

func (t *TagStatOs) Process(ctx context.Context) {
	baseProcess(ctx, t, MetricOs, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}
func (t *TagStatOs) ProcessSql() ([]string, error) {
	return baseProcessSqls(t, MetricOs, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}

func (t *TagStatOs) processI(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatOs{
		TagId: t.TagId(),
		Date:  dt,
	}
	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricImpressions)
		ret.Scan(&s.Name, &s.OsType, &s.Impressions)
	})
}

func (t *TagStatOs) processC(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatOs{
		TagId: t.TagId(),
		Date:  dt,
	}
	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricClick)
		ret.Scan(&s.Name, &s.OsType, &s.Clicks)
	})
}

func (t *TagStatOs) processS(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatOs{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricSession)
		ret.Scan(&s.Name, &s.OsType, &s.Sessions)
	})
}

func (t *TagStatOs) processP(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatOs{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricPageviews)
		ret.Scan(&s.Name, &s.OsType, &s.Pageviews)
	})
}

func (t *TagStatOs) processV(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatOs{
		TagId: t.TagId(),
		Date:  dt,
	}
	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricVisitors)
		ret.Scan(&s.Name, &s.OsType, &s.Visitors)
	})
}
