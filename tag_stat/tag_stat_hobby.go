package tag_stat

import (
	"context"

	"dmp_web/go/commons/db/hive"
	"dmp_web/go/model"
)

var MetricHobby = []*Metric{
	{"hobby", model.MetricValue, "dt", model.NoneProduct, []int{PC},
		`
SELECT t6.name1 AS interest,
       t5.cnt
FROM (
SELECT catid,
       count(1) AS cnt
FROM
    (SELECT dubc.userid,
            split(dubc.baidu_usercategory,'\\|') AS ucats
     FROM dmp.user_baidu_category AS dubc
     JOIN  %[2]v   AS t3 ON (t3.%[4]v = dubc.userid)) AS t4 LATERAL VIEW explode(t4.ucats) r1 AS catid
     WHERE catid IN (606, 790, 518, 508, 343, 148, 910, 521, 609, 522, 184, 147, 91, 399, 927, 391, 248, 446, 266, 393, 303, 394, 397, 274, 525, 528)
     GROUP BY catid) AS t5
JOIN dmp.dim_baidu_category AS t6 ON (t5.catid = t6.Id
                                      AND t6.Id IS NOT NULL)

	`},
	//	{"hobby", model.MetricTotal, "dt", model.NoneProduct,
	//		`
	//SELECT count(1) AS cnt
	//FROM
	//    (SELECT dubc.userid
	//     FROM dmp.user_baidu_category AS dubc
	//     JOIN %[2]v AS t1 ON (t1.%[4]v = dubc.userid
	//                          AND t1.%[4]v IS NOT NULL)
	//     GROUP BY dubc.userid)
	//	`},
	{"hobby", model.MetricImpressions, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT dim_baidu_category.name1,
       uca.cnt
FROM (
SELECT uc.catid,
       count(1) AS cnt
FROM
    (SELECT t4.userid,
            catid
     FROM
         (SELECT dubc.userid,
                 split(dubc.baidu_usercategory,'\\|') AS ucats
          FROM dmp.user_baidu_category AS dubc
          JOIN %[2]v AS t3 ON (t3.%[4]v = dubc.userid
    )) AS t4 LATERAL VIEW explode(t4.ucats) r1 AS catid
          WHERE catid IN (606, 790, 518, 508, 343, 148, 910, 521, 609, 522, 184, 147, 91, 399, 927, 391, 248, 446, 266, 393, 303, 394, 397, 274, 525, 528)) AS uc
     INNER JOIN     (SELECT visitorid
     FROM dsp.dw_ana_logs
     WHERE %[1]v
         AND aduserid IN (%[3]v) ) as ca ON uc.userid = ca.visitorid
     GROUP BY uc.catid) AS uca
INNER JOIN dmp.dim_baidu_category ON (uca.catid = dim_baidu_category.id
                                      AND uca.catid IS NOT NULL)
`},
	{"hobby", model.MetricClick, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT dim_baidu_category.name1,
       uca.cnt
FROM (
SELECT uc.catid,
       count(1) AS cnt
FROM
    (SELECT t4.userid,
            catid
     FROM
         (SELECT dubc.userid,
                 split(dubc.baidu_usercategory,'\\|') AS ucats
          FROM dmp.user_baidu_category AS dubc
          JOIN %[2]v AS t3 ON (t3.%[4]v = dubc.userid
    )) AS t4 LATERAL VIEW explode(t4.ucats) r1 AS catid
          WHERE catid IN (606, 790, 518, 508, 343, 148, 910, 521, 609, 522, 184, 147, 91, 399, 927, 391, 248, 446, 266, 393, 303, 394, 397, 274, 525528)) AS uc
     INNER JOIN    (SELECT visitorid
     FROM dsp.dw_whisky_logs
     WHERE %[1]v
         AND aduserid IN (%[3]v) ) as ck ON uc.userid = ck.visitorid
     GROUP BY uc.catid) AS uca
INNER JOIN dmp.dim_baidu_category ON (uca.catid = dim_baidu_category.id
                                      AND uca.catid IS NOT NULL)
`},
	{"hobby", model.MetricValue, "dt", model.NoneProduct, []int{Mob},
		`
SELECT t6.name1 AS interest,
       t5.cnt
FROM (
SELECT catid,
       count(1) AS cnt
FROM
    (SELECT dubc.did,
            split(dubc.baidu_usercategory,'\\|') AS ucats
     FROM dmp.user_baidu_category_mobile AS dubc
     JOIN  %[2]v   AS t3 ON (t3.%[4]v = dubc.did)) AS t4 LATERAL VIEW explode(t4.ucats) r1 AS catid
     WHERE catid IN (606, 790, 518, 508, 343, 148, 910, 521, 609, 522, 184, 147, 91, 399, 927, 391, 248, 446, 266, 393, 303, 394, 397, 274, 525, 528)
     GROUP BY catid) AS t5
JOIN dmp.dim_baidu_category AS t6 ON (t5.catid = t6.Id
                                      AND t6.Id IS NOT NULL)

	`},
	//	{"hobby", model.MetricTotal, "dt", model.NoneProduct,
	//		`
	//SELECT count(1) AS cnt
	//FROM
	//    (SELECT dubc.did
	//     FROM dmp.user_baidu_category_mobile AS dubc
	//     JOIN %[2]v AS t1 ON (t1.%[4]v = dubc.did
	//                          AND t1.%[4]v IS NOT NULL)
	//     GROUP BY dubc.did)
	//	`},
	{"hobby", model.MetricImpressions, "dt", []model.Product{model.ProductDSP}, []int{Mob},
		`
SELECT dim_baidu_category.name1,
       uca.cnt
FROM (
SELECT uc.catid,
       count(1) AS cnt
FROM
    (SELECT t4.did,
            catid
     FROM
         (SELECT dubc.did,
                 split(dubc.baidu_usercategory,'\\|') AS ucats
          FROM dmp.user_baidu_category_mobile AS dubc
          JOIN %[2]v AS t3 ON (t3.%[4]v = dubc.did
    )) AS t4 LATERAL VIEW explode(t4.ucats) r1 AS catid
          WHERE catid IN (606, 790, 518, 508, 343, 148, 910, 521, 609, 522, 184, 147, 91, 399, 927, 391, 248, 446, 266, 393, 303, 394, 397, 274, 525, 528)) AS uc
     INNER JOIN     (SELECT visitorid
     FROM dsp.dw_ana_logs
     WHERE %[1]v
         AND aduserid IN (%[3]v) ) as ca ON uc.did = ca.visitorid
     GROUP BY uc.catid) AS uca
INNER JOIN dmp.dim_baidu_category ON (uca.catid = dim_baidu_category.id
                                      AND uca.catid IS NOT NULL)
`},
	{"hobby", model.MetricClick, "dt", []model.Product{model.ProductDSP}, []int{Mob},
		`
SELECT dim_baidu_category.name1,
       uca.cnt
FROM
    (SELECT uc.catid,
            count(1) AS cnt
     FROM
         (SELECT t4.did,
                 catid
          FROM
              (SELECT dubc.did,
                      split(dubc.baidu_usercategory,'\\|') AS ucats
               FROM dmp.user_baidu_category_mobile AS dubc
               JOIN %[2]v AS t3 ON (t3.%[4]v = dubc.did )) AS t4 LATERAL VIEW explode(t4.ucats) r1 AS catid
          WHERE catid IN (606, 790, 518, 508, 343, 148, 910, 521, 609, 522, 184, 147, 91, 399, 927, 391, 248, 446, 266, 393, 303, 394, 397, 274, 525528)) AS uc
     INNER JOIN
         (SELECT visitorid
          FROM dsp.dw_whisky_logs
          WHERE %[1]v
              AND aduserid IN (%[3]v) ) AS ck ON uc.did = ck.visitorid
     GROUP BY uc.catid) AS uca
INNER JOIN dmp.dim_baidu_category ON (uca.catid = dim_baidu_category.id
                                      AND uca.catid IS NOT NULL)
`},
}

type TagStatHobby struct {
	*Dimension
}

func (t *TagStatHobby) GetFunc(metric string) ProcessFunc {
	switch metric {
	case model.MetricValue:
		return t.processV
	case model.MetricImpressions:
		return t.processImp
	case model.MetricClick:
		return t.processClk
	}
	return nil
}

func (t *TagStatHobby) GetModel() model.StatModel {
	return &model.StatHobbyModel
}

func (t *TagStatHobby) Process(ctx context.Context) {
	baseProcess(ctx, t, MetricHobby, &ProcessConfig{SuppliersStrs: t.Dimension.suppliers})
}
func (t *TagStatHobby) ProcessSql() ([]string, error) {
	return baseProcessSqls(t, MetricHobby, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}

func (t *TagStatHobby) processV(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatHobby{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricValue)
		ret.Scan(&s.Name, &s.Value)
	})
}

func (t *TagStatHobby) processT(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatHobby{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricTotal)
		ret.Scan(&s.Total)
	})
}

func (t *TagStatHobby) processImp(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	//self.GetModel().Clean(model.MetricImpressions, self.TagId(), "")
	obj := &model.StatHobby{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricImpressions)
		ret.Scan(&s.Total)
	})
}

func (t *TagStatHobby) processClk(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatHobby{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricClick)
		ret.Scan(&s.Total)
	})
}
