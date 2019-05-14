package tag_stat

import (
	"context"
	"dmp_web/go/commons/db/hive"
	"dmp_web/go/model"
)

// MetricRegion 统计地域维度的 SQL
var MetricRegion = []*Metric{
	{"region", model.MetricSession, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
	SELECT t5.location,
	       count(1) AS cnt
	FROM
	    (SELECT t2.name AS LOCATION,
	            t1.%[4]v
	     FROM
	         (SELECT (CASE
	                      WHEN country_id = 10761 THEN concat(country_id, "$", province_id)
	                      ELSE country_id
	                  END) AS id,
	                 %[4]v
	          FROM dna_v4.dna_v4_session_base
	          WHERE %[1]v
	              AND cast(dna_v4_session_base.%[4]v AS string) IN
	                  (SELECT %[4]v
	                   FROM %[2]v)
	              AND %[4]v IS NOT NULL
	              AND dna_v4_session_base.ad_sf_aduser_id IN (%[3]v)
	          GROUP BY %[4]v,
	                   session_id,
	                   country_id,
	                   province_id) t1
	     JOIN
	         (SELECT id,
	                 name
	          FROM
	              (SELECT (CASE
	                           WHEN country = 10761 THEN concat(country, "$", region)
	                           ELSE country
	                       END) AS id,
	                      (CASE
	                           WHEN country = 10761 THEN concat(country_name_cn, region_name_cn)
	                           ELSE country_name_cn
	                       END) AS name
	               FROM dsp.dim_location) t4
	          GROUP BY id,
	                   name) t2 ON (t1.id = t2.id
	                                AND t1.id IS NOT NULL AND t1.id !="")) AS t5
	GROUP BY t5.location
		`},

	{"region", model.MetricPageviews, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
	SELECT t5.location,
	       count(1) AS cnt
	FROM
	    (SELECT t2.name AS LOCATION,
	            t1.%[4]v
	     FROM
	         (SELECT (CASE
	                      WHEN country_id = 10761 THEN concat(country_id, "$", province_id)
	                      ELSE country_id
	                  END) AS id,
	                 %[4]v
	          FROM dna_v4.dna_v4_session_base
	          WHERE %[1]v
	              AND cast(dna_v4_session_base.%[4]v AS string) IN
	                  (SELECT %[4]v
	                   FROM %[2]v)
	              AND %[4]v IS NOT NULL
	              AND dna_v4_session_base.ad_sf_aduser_id IN (%[3]v) ) t1
	     JOIN
	         (SELECT id,
	                 name
	          FROM
	              (SELECT (CASE
	                           WHEN country = 10761 THEN concat(country, "$", region)
	                           ELSE country
	                       END) AS id,
	                      (CASE
	                           WHEN country = 10761 THEN concat(country_name_cn, region_name_cn)
	                           ELSE country_name_cn
	                       END) AS name
	               FROM dsp.dim_location) t4
	          GROUP BY id,
	                   name) t2 ON (t1.id = t2.id
	                                AND t1.id IS NOT NULL AND t1.id !="")) AS t5
	GROUP BY t5.location
		`},
	{"region", model.MetricVisitors, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
	 SELECT t5.location,
	        count(1) AS cnt
	 FROM
	     (SELECT t2.name AS LOCATION,
	             t1.%[5]v
	      FROM
	          (SELECT (CASE
	                       WHEN geo["country"] = 10761 THEN concat(geo["country"], "$", geo["region"])
	                       ELSE geo["country"]
	                   END) AS id,
	                  %[5]v
	           FROM
	               (SELECT udf.geo(ip) AS geo,
	                       %[5]v
	                FROM dsp.dw_ana_logs
	                WHERE %[1]v
	                    AND dw_ana_logs.%[5]v IN
	                        (SELECT %[4]v
	                         FROM %[2]v
	                         GROUP BY %[4]v)
	                    AND %[5]v IS NOT NULL
	                    AND dw_ana_logs.aduserid IN (%[3]v)
	                GROUP BY ip,
	                         %[5]v) AS t3
	           UNION ALL SELECT (CASE
	                                 WHEN country_id = 10761 THEN concat(country_id, "$", province_id)
	                                 ELSE country_id
	                             END) AS id,
	                            %[4]v AS %[5]v
	           FROM dna_v4.dna_v4_session_base
	           WHERE %[1]v
	               AND cast(dna_v4_session_base.%[4]v AS string) IN
	                   (SELECT %[4]v
	                    FROM %[2]v)
	               AND %[4]v IS NOT NULL
	               AND dna_v4_session_base.ad_sf_aduser_id IN (%[3]v)
	           GROUP BY country_id,
	                    province_id,
	                    %[4]v) t1
	      JOIN
	          (SELECT id,
	                  name
	           FROM
	               (SELECT (CASE
	                            WHEN country = 10761 THEN concat(country, "$", region)
	                            ELSE country
	                        END) AS id,
	                       (CASE
	                            WHEN country = 10761 THEN concat(country_name_cn, region_name_cn)
	                            ELSE country_name_cn
	                        END) AS name
	                FROM dsp.dim_location) t4
	           GROUP BY id,
	                    name) t2 ON (t1.id = t2.id
	                                 AND t1.id IS NOT NULL AND t1.id !="")) AS t5
	 GROUP BY t5.location
	 	`},

	{"region", model.MetricImpressions, "dt", []model.Product{model.ProductDSP}, []int{PC, Mob},
		`
SELECT t5.location,
       count(1) AS cnt
FROM
    (SELECT t2.name AS LOCATION,
            t1.%[5]v
     FROM
         (SELECT (CASE
                      WHEN geo["country"] = 10761 THEN concat(geo["country"], "$", geo["region"])
                      ELSE geo["country"]
                  END) AS id,
                 %[5]v
          FROM
              (SELECT udf.geo(ip) AS geo,
                      %[5]v
               FROM dsp.dw_ana_logs
               WHERE %[1]v
                   AND dw_ana_logs.%[5]v IN
                       (SELECT %[4]v
                        FROM %[2]v)
                   AND %[5]v IS NOT NULL
                   AND dw_ana_logs.aduserid IN (%[3]v) ) AS t3) t1
     JOIN
         (SELECT id,
                 name
          FROM
              (SELECT (CASE
                           WHEN country = 10761 THEN concat(country, "$", region)
                           ELSE country
                       END) AS id,
                      (CASE
                           WHEN country = 10761 THEN concat(country_name_cn, region_name_cn)
                           ELSE country_name_cn
                       END) AS name
               FROM dsp.dim_location) t4
          GROUP BY id,
                   name) t2 ON (t1.id = t2.id
                                AND t1.id IS NOT NULL AND t1.id !="")) AS t5
GROUP BY t5.location
`},

	{"region", model.MetricClick, "dt", []model.Product{model.ProductDSP}, []int{PC, Mob},
		`
SELECT t5.location,
       count(1) AS cnt
FROM
    (SELECT t2.name AS LOCATION,
            t1.%[5]v
     FROM
         (SELECT (CASE
                      WHEN geo["country"] = 10761 THEN concat(geo["country"], "$", geo["region"])
                      ELSE geo["country"]
                  END) AS id,
                 %[5]v
          FROM
              (SELECT udf.geo(ip) AS geo,
                      %[5]v
               FROM dsp.dw_whisky_logs
               WHERE %[1]v
                   AND dw_whisky_logs.%[5]v IN
                       (SELECT %[4]v
                        FROM %[2]v)
                   AND %[5]v IS NOT NULL
                   AND dw_whisky_logs.aduserid IN (%[3]v) ) AS t3) t1
     JOIN
         (SELECT id,
                 name
          FROM
              (SELECT (CASE
                           WHEN country = 10761 THEN concat(country, "$", region)
                           ELSE country
                       END) AS id,
                      (CASE
                           WHEN country = 10761 THEN concat(country_name_cn, region_name_cn)
                           ELSE country_name_cn
                       END) AS name
               FROM dsp.dim_location) t4
          GROUP BY id,
                   name) t2 ON (t1.id = t2.id
                                AND t1.id IS NOT NULL AND t1.id !="")) AS t5
GROUP BY t5.location
 `},
}

// TagStatRegion ...
type TagStatRegion struct {
	*Dimension
}

// GetFunc 根据 metric 的值统计不同维度的数据
func (t *TagStatRegion) GetFunc(metric string) ProcessFunc {
	switch metric {
	case model.MetricSession:
		return t.processS
	case model.MetricPageviews:
		return t.processP
	case model.MetricVisitors:
		return t.processV
	case model.MetricImpressions:
		return t.processI
	case model.MetricClick:
		return t.processC
	}
	return nil
}

// GetModel 返回 StatRegion model
func (t *TagStatRegion) GetModel() model.StatModel {
	return &model.StatRegionModel
}

// Process 处理统计任务
func (t *TagStatRegion) Process(ctx context.Context) {
	baseProcess(ctx, t, MetricRegion, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}
func (t *TagStatRegion) ProcessSql() ([]string, error) {
	return baseProcessSqls(t, MetricRegion, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}

func (t *TagStatRegion) processS(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatRegion{
		TagId: t.TagId(),
		Date:  dt,
	}
	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricSession)
		ret.Scan(&s.Name, &s.Sessions)
	})
}

func (t *TagStatRegion) processP(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatRegion{
		TagId: t.TagId(),
		Date:  dt,
	}
	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricPageviews)
		ret.Scan(&s.Name, &s.Pageviews)
	})
}

func (t *TagStatRegion) processV(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatRegion{
		TagId: t.TagId(),
		Date:  dt,
	}
	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricVisitors)
		ret.Scan(&s.Name, &s.Visitors)
	})
}

func (t *TagStatRegion) processC(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatRegion{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricClick)
		ret.Scan(&s.Name, &s.Clicks)
	})
}

func (t *TagStatRegion) processI(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatRegion{
		TagId: t.TagId(),
		Date:  dt,
	}
	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricImpressions)
		ret.Scan(&s.Name, &s.Impressions)
	})
}
