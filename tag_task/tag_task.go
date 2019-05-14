package tag_task

import (
	"dmp_web/go/commons/env"
	"dmp_web/go/commons/errors"
	"dmp_web/go/model"
	"dmp_web/go/model/model_rule"
)

func NewExtendConfig(date *model_rule.Date, suppliers []int, vType model_rule.Vtype) (*model_rule.ExtendConfig, error) {
	if len(suppliers) == 0 {
		return nil, errors.Newf("empty supplier")
	}

	// 将供应商的内部ID转换成实际外部ID
	supplier, err := model.SupplierModel.Transform(suppliers)

	if err != nil {
		return nil, err
	}

	if len(supplier) != len(suppliers) {
		return nil, errors.Newf(
			"some of suppliers is missing, %v/%v", len(supplier), len(suppliers))
	}

	return &model_rule.ExtendConfig{
		Date:     date,
		Type:     vType,
		Supplier: supplier,
	}, nil
}

//  用于提供额外的条件限制
//   供应商
//   时间范围
//   visitorId 类型
func TagToExtendConfig(tag *model.Tag) (*model_rule.ExtendConfig, error) {
	date, err := tag.GetDate()
	if err != nil {
		return nil, err
	}
	return NewExtendConfig(date, tag.SupplierIds, tag.GetVtype())
}

func QueryPlan(tag *model.Tag) ([]string, error) {
	extend, err := TagToExtendConfig(tag)
	if err != nil {
		return nil, err
	}
	tagStage := NewTagStage("tag", tag, env.HiveDatabase, extend)
	if err != nil {
		return nil, err
	}
	if err := tagStage.Init(); err != nil {
		return nil, err
	}
	return tagStage.GroupPlan(), nil

}

func QueryPlanDebug(tag *model.Tag) (string, error) {
	extend, err := TagToExtendConfig(tag)
	if err != nil {
		return "", err
	}
	tagStage := NewTagStage("tag", tag, env.HiveDatabase, extend)
	if err != nil {
		return "", err
	}
	if err := tagStage.Init(); err != nil {
		return "", err
	}
	return tagStage.QueryPlanDebug(), nil

}
