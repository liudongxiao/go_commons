package tag_task

import (
	"context"
	"dmp_web/go/commons/db/hive"
	"dmp_web/go/commons/env"
	"dmp_web/go/model"
	"dmp_web/go/model/model_rule"

	"testing"
)

func saveSupplier() {
	s1 := &model.Supplier{
		Id:        9990,
		AccountId: 19990,
	}
	model.SupplierModel.SaveId(&s1.Id, s1)
	s1 = &model.Supplier{
		Id:        9991,
		AccountId: 19991,
	}
	model.SupplierModel.SaveId(&s1.Id, s1)
	s1 = &model.Supplier{
		Id:        9992,
		AccountId: 19992,
	}
	model.SupplierModel.SaveId(&s1.Id, s1)
}

func saveTag(t *testing.T, tag *model.Tag) {
	ctx, _ := context.WithCancel(context.Background())
	if err := model.TagModel.SaveId(&tag.Id, tag); err != nil {
		t.Error(err)
	}

	var cfg *hive.Config
	env.GetTestConf(&cfg)
	hcli, err := hive.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	extcfg, err := TagToExtendConfig(tag)
	if err != nil {
		t.Fatal(err)
	}
	ts := NewTagStage("test", tag, "dmpstage", extcfg)
	if err := ts.Init(); err != nil {
		t.Fatal(err)
	}
	if err := ts.Run(ctx, hcli); err != nil {
		t.Fatal(err)
	}
}

var tag1 = &model.Tag{
	Id:          10240001,
	Name:        "30天看过广告的人",
	DateType:    1,
	DateRela:    30,
	TypeId:      1,
	SupplierIds: []int{9990, 9991, 9992},
	Rule: &model_rule.Rule{
		Key: "$and",
		Value: model_rule.RulesValue{
			{
				Key: "campaign",
				Op:  model_rule.OpIn,
				Value: model_rule.Int64sValue{
					10001, 10002,
				},
			},
			{
				Key: "$or",
				Value: model_rule.RulesValue{
					{
						Key:   "mediaDomain",
						Op:    model_rule.OpEq,
						Value: model_rule.StringValue("hello"),
					},
					{
						Key: "campaign",
						Op:  model_rule.OpIn,
						Value: model_rule.Int64sValue{
							10003, 10004,
						},
					},
				},
			},
		},
	},
}

var tag = &model.Tag{
	Id:                  10240000,
	Name:                "点击曝光",
	DateType:            1,
	DateRela:            30,
	TypeId:              2,
	SupplierIds:         []int{9990, 9991, 9992},
	PeopleSumary:        true,
	AdvertisementSumary: true,
	LandingPageSumary:   true,
	CostingSumary:       true,
	Rule: &model_rule.Rule{
		Key: "$and",
		Value: model_rule.RulesValue{
			{
				Key:   "clickCount",
				Op:    model_rule.OpLt,
				Value: model_rule.IntValue(1),
			},
			{
				Key:   "exposeCount",
				Op:    model_rule.OpGte,
				Value: model_rule.IntValue(1),
			},
		},
	},
}

var tag2 = &model.Tag{
	Id:          10240002,
	Name:        "引用规则",
	DateType:    1,
	DateRela:    30,
	TypeId:      2,
	SupplierIds: []int{9990, 9991, 9992},
	Rule: &model_rule.Rule{
		Key: "$and",
		Value: model_rule.RulesValue{
			{
				Key:   "tag",
				Op:    model_rule.OpIn,
				Value: model_rule.Int64sValue{},
			},
		},
	},
}
var tag3 = &model.Tag{
	Id:          10240003,
	Name:        "引用规则",
	DateType:    1,
	DateRela:    30,
	TypeId:      2,
	SupplierIds: []int{9990, 9991, 9992},
	Rule: &model_rule.Rule{
		Key: "$and",
		Value: model_rule.RulesValue{
			{
				Key:   "tag",
				Op:    model_rule.OpIn,
				Value: model_rule.Int64sValue{1},
			},
		},
	},
}

var testTags = []*model.Tag{tag, tag1, tag2, tag3}

// 采用和 hive 类似的分阶段执行任务，最后通过 stage 的依赖完成处理
func TestTagSyntaxTree(t *testing.T) {
	saveSupplier()

	saveTag(t, tag)
}

func TestTagRun(t *testing.T) {
	if err := model.TagModel.SaveId(&tag.Id, tag); err != nil {
		t.Error(err)
	}

	var cfg *hive.Config
	env.GetTestConf(&cfg)
	hcli, err := hive.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, _ := context.WithCancel(context.Background())
	extend, err := TagToExtendConfig(tag)
	if err != nil {
		t.Fatalf("err on extend%v: %+v", tag, err)
	}
	tagStage := NewTagStage("tag", tag, env.HiveDatabase, extend)
	if err != nil {
		t.Fatalf("err on extend%v: %+v", tag, err)
	}
	if err := tagStage.Init(); err != nil {
		t.Fatalf("err on extend%v: %+v", tag, err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := tagStage.Run(ctx, hcli); err != nil {
		t.Fatal(err)
	}
}

func TestDebug(t *testing.T) {
	for _, tag := range testTags {
		extend, err := TagToExtendConfig(tag)
		if err != nil {
			t.Fatalf("err on extend%v: %+v", tag, err)
		}
		tagStage := NewTagStage("tag", tag, env.HiveDatabase, extend)
		if err != nil {
			t.Fatalf("err on extend%v: %+v", tag, err)
		}
		if err := tagStage.Init(); err != nil {
			t.Fatalf("err on extend%v: %+v", tag, err)
		}
		t.Log(tagStage.QueryPlanDebug())
		//t.Log(tagStage.GroupPlan())
	}
}
func TestTag(t *testing.T) {
	saveSupplier()
	extend, err := TagToExtendConfig(tag)
	if err != nil {
		t.Fatalf("err on extend%v: %+v", tag, err)
	}
	tagStage := NewTagStage("tag", tag, env.HiveDatabase, extend)
	if err != nil {
		t.Fatalf("err on extend%v: %+v", tag, err)
	}
	if err := tagStage.Init(); err != nil {
		t.Fatalf("err on extend%v: %+v", tag, err)
	}

	t.Log(tagStage.QueryPlanDebug())
}
