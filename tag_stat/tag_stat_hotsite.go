package tag_stat

import (
	"context"
	"dmp_web/go/model"

	"dmp_web/go/commons/db/hive"
)

var MetricHotsite = []*Metric{
	{"hotsite", model.MetricVisitors, "dt", model.NoneProduct, []int{PC},
		`
SELECT t2.domain,
       count(1) AS cnt
FROM
    (SELECT %[4]v
     FROM %[2]v
     GROUP BY %[4]v) AS t1
JOIN dsp.dw_user_bid_domain AS t2 ON (t1.%[4]v = t2.userid
                                      AND t1.%[4]v IS NOT NULL)
GROUP BY t2.domain
`},
}

type TagStatHotsite struct {
	*Dimension
}

func (t *TagStatHotsite) GetFunc(metric string) ProcessFunc {
	switch metric {
	case model.MetricVisitors:
		return t.processV
	}
	return nil
}

func (t *TagStatHotsite) GetModel() model.StatModel {
	return &model.StatHotsiteModel
}

func (t *TagStatHotsite) Process(ctx context.Context) {
	baseProcess(ctx, t, MetricHotsite, &ProcessConfig{SuppliersStrs: t.Dimension.suppliers})
}
func (t *TagStatHotsite) ProcessSql() ([]string, error) {
	return baseProcessSqls(t, MetricHotsite, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}

func (t *TagStatHotsite) processV(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatHotsite{
		TagId: t.TagId(),
		Date:  dt,
	}

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricVisitors)
		ret.Scan(&s.Name, &s.Visitors)
	})
}
