package tag_stat

import (
	"dmp_web/go/commons/env"
	"dmp_web/go/model"
	"dmp_web/go/model/model_rule"
	"testing"
)

func TestNewAnalyseStat(t *testing.T) {
	type args struct {
		isNew       int
		Id           int64
		campaignItem *model.CampaignItem
	}
	tests := []struct {
		name string
		args args
	}{
		{"coustom", args{1,int64(10240000), &model.CampaignItem{CampaignIds: []int{515526, 515527}, Type: env.DspMobStr,Date: "20190122"}}},
		{"normal_pc", args{0, int64(10240000), &model.CampaignItem{CampaignIds: []int{515526, 515527}, Type: env.PCStr, Date: "20190122"}}},
		{"normal_mob", args{0, int64(10240000), &model.CampaignItem{CampaignIds: []int{515526, 515527}, Type: env.DspMobStr, Date: "20190122"}}},
	}
	saveTag(t, tag)
	saveTagStage(t, tagStage)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewAnalyseStatWithTag(tt.args.isNew, tt.args.Id, tt.args.campaignItem)
			if err != nil {
				t.Error(err)
			} else {
				t.Logf("NewCampaignStat() = %+v", *got)
			}
		})
	}
}

func saveTag(t *testing.T, tag *model.Tag) {
	if err := model.TagModel.SaveId(&tag.Id, tag); err != nil {
		t.Error(err)
	}
}

func saveTagStage(t *testing.T, ts *model.TagStage) {
	if err := model.TagStageModel.SaveId(&ts.Id, tagStage); err != nil {
		t.Error(err)
	}
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

var tagStage = &model.TagStage{
	Id:  10240000,
	Tbl: "dmpstage.tag_10240000_sid_9990_9991_9992_type_2_date_20190128_20190227_sqlv_2762208907_hash_4106586910",
}

func TestNewCampaignStat(t *testing.T) {
	type args struct {
		isNew       int
		Id           int64
		campaignItem *model.CampaignItem
	}
	tests := []struct {
		name string
		args args
	}{
		{"coustom", args{1, int64(0), &model.CampaignItem{CampaignIds: []int{515526, 515527}, Type: env.DspMobStr, Date: "20190122"}}},
		{"normal_pc", args{0, int64(0), &model.CampaignItem{CampaignIds: []int{515526, 515527}, Type: env.PCStr, Date: "20190122"}}},
		{"normal_mob", args{0, int64(0), &model.CampaignItem{CampaignIds: []int{515526, 515527}, Type: env.DspMobStr, Date: "20190122"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewAnalyseStatWithTag(tt.args.isNew, tt.args.Id, tt.args.campaignItem)
			if err != nil {
				t.Error(err)
			} else {
				t.Logf("NewCampaignStat() = %+v", *got)
			}
		})
	}
}
