package tag_stat

import (
	"context"
	"dmp_web/go/model"
	"fmt"

	"dmp_web/go/commons/db/hive"
)

var MetricFrequency = []*Metric{
	{"frequency", model.MetricVisitors, "dt", []model.Product{model.ProductDSP}, []int{PC},
		`
SELECT t1.frequency,
       count(1) AS cnt
FROM
    (SELECT %[4]v,
            count(1) AS frequency
     FROM dsp.dw_ana_logs
     WHERE %[1]v
         AND dw_ana_logs.%[4]v IN
             (SELECT visitor_id
              FROM %[2]v
              GROUP BY visitor_id)
         AND dw_ana_logs.aduserid IN (%[3]v)
         AND %[4]v IS NOT NULL
     GROUP BY %[4]v) AS t1
GROUP BY t1.frequency
 `},
}

type TagStatFrequency struct {
	*Dimension
}

func (t *TagStatFrequency) GetFunc(metric string) ProcessFunc {
	switch metric {
	case model.MetricVisitors:
		return t.processVI
	}
	return nil
}

func (t *TagStatFrequency) GetModel() model.StatModel {
	return &model.StatFrequencyModel
}

func (t *TagStatFrequency) Process(ctx context.Context) {
	baseProcess(ctx, t, MetricFrequency, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}
func (t *TagStatFrequency) ProcessSql() ([]string, error) {
	return baseProcessSqls(t, MetricFrequency, &ProcessConfig{Date: t.Dimension.date, SuppliersStrs: t.Dimension.suppliers, etype: t.tag.TypeId})
}

func (t *TagStatFrequency) processVI(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	t.GetModel().Clean(model.MetricImpressions, t.TagId(), dt)

	obj := &model.StatFrequency{
		TagId: t.TagId(),
		Date:  dt,
	}

	var hasTenPlus bool
	var tenPlusVisitor int64
	var tenPlusImpression int64

	size, err := t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		var frequency int64
		var visitors int64
		ret.Scan(&frequency, &visitors)
		if frequency >= 10 {
			hasTenPlus = true
			tenPlusVisitor += visitors
			tenPlusImpression += visitors * frequency
			return
		}
		s := obj.New(data, model.MetricVisitors)
		s.Name = fmt.Sprint(frequency)
		s.Visitors = visitors
		s = s.New(data, model.MetricImpressions)
		s.Impressions = frequency * visitors
	})
	if err != nil {
		return -1, err
	}
	if hasTenPlus {
		objV := *obj
		objV.Name = ">10"
		objV.Metric = model.MetricVisitors
		objV.Visitors = tenPlusVisitor
		objI := *obj
		objI.Name = ">10"
		objI.Impressions = tenPlusImpression
		objI.Metric = model.MetricImpressions

		if err := t.GetModel().Insert([]interface{}{&objV, &objI}); err != nil {
			return -1, err
		}
	}
	return size, nil
}
