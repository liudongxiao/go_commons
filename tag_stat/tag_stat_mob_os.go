package tag_stat

import (
	"context"

	"dmp_web/go/commons/db/hive"
	"dmp_web/go/model"
	"dmp_web/go/model/model_rule"
)

var MetricMobOs = []*Metric{
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
	//   dsp.dim_ostype t3
	// on (t2.ostype = t3.id  and t3.id  is not null)
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
	//   dsp.dim_ostype t3
	// on (t2.ostype = t3.id  and t3.id  is not null)

	// 	`},

	{dimension: "mob_os",
		Name:                   model.MetricValue,
		DateField:              "dt",
		SupplierDependProducts: model.NoneProduct,
		etype:                  []int{Mob},
		Layout: `
SELECT ds.os,
       count(1) AS cnt
     FROM %[2]v  AS ds
GROUP BY ds.os
`},

	{
		dimension:              "mob_os",
		Name:                   model.MetricImpressions,
		DateField:              "dt",
		SupplierDependProducts: []model.Product{model.ProductDSP},
		etype:                  []int{Mob},
		Layout: `
SELECT ds.os,
       count(1)
FROM %[2]v AS ds
INNER  JOIN
    (SELECT %[5]v AS %[4]v
     FROM dsp.dw_ana_logs
     WHERE %[1]v
         AND aduserid IN (%[3]v) ) AS ca ON (ds.%[4]v = ca.%[4]v)
GROUP BY ds.os
`},

	{
		dimension:              "mob_os",
		Name:                   model.MetricClick,
		DateField:              "dt",
		SupplierDependProducts: []model.Product{model.ProductDSP},
		etype:                  []int{Mob},
		Layout: `
SELECT ds.os,
       count(1)
FROM %[2]v AS ds
INNER  JOIN
    (SELECT %[5]v AS %[4]v
     FROM dsp.dw_whisky_logs
     WHERE %[1]v
         AND aduserid IN (%[3]v) ) AS ck ON (ds.%[4]v = ck.%[4]v)
GROUP BY ds.os
`,
	},
}

type TagStatMobOs struct {
	*Dimension
}

func (t *TagStatMobOs) GetFunc(metric string) ProcessFunc {
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

//func (t *TagStatMobOs) GetFunc(metric string) ProcessFunc {
//	return func(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
//		obj := &model.StatOs{
//			TagId: t.TagId(),
//			ToDate:  dt,
//		}
//
//		return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
//			s := obj.New(data, metric)
//			ret.Scan(&s.Name, &s.Value)
//		})
//	}
//}

func (t *TagStatMobOs) GetModel() model.StatModel {
	return &model.StatOsModel
}

func (t *TagStatMobOs) Process(ctx context.Context) {
	baseProcess(ctx, t, MetricOs, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}
func (t *TagStatMobOs) ProcessSql() ([]string, error) {
	return baseProcessSqls(t, MetricOs, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}

func (t *TagStatMobOs) processI(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatOs{
		TagId: t.TagId(),
		Date:  dt,
	}
	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricImpressions)
		ret.Scan(&s.Name, &s.OsType, &s.Impressions)
	})
}

func (t *TagStatMobOs) processC(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatOs{
		TagId: t.TagId(),
		Date:  dt,
	}
	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricClick)
		ret.Scan(&s.Name, &s.OsType, &s.Clicks)
	})
}

func (t *TagStatMobOs) processS(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatOs{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricSession)
		ret.Scan(&s.Name, &s.OsType, &s.Sessions)
	})
}

func (t *TagStatMobOs) processP(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatOs{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricPageviews)
		ret.Scan(&s.Name, &s.OsType, &s.Pageviews)
	})
}

func (t *TagStatMobOs) processV(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatOs{
		TagId: t.TagId(),
		Date:  dt,
	}
	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricVisitors)
		ret.Scan(&s.Name, &s.OsType, &s.Visitors)
	})
}

// suppliersGroup： []string{"111,222,333", "444,555", "666,777,888"}, 分别使用layout中的 3、4、5号占位符
func (t *TagStatMobOs) StatementByRange(layout string, field string, date *model_rule.Date,
	suppliersGroup []string, name string) (string, error) {
	return t.StatementByRangeWithBaiduTable(layout, field, date, suppliersGroup, name)
}
