package tag_stat

import (
	"context"
	"dmp_web/go/model"
	"dmp_web/go/model/model_rule"
	"reflect"
	"testing"

	"gopkg.in/mgo.v2/bson"
)

func Test_getSuppliersGroup(t *testing.T) {
	type args struct {
		productToSuppliers map[model.Product]string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getSuppliersGroup(tt.args.productToSuppliers); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getSuppliersGroup() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getValidProducts(t *testing.T) {
	type args struct {
		cfg *ProcessConfig
	}
	tests := []struct {
		name string
		args args
		want []model.Product
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getValidProducts(tt.args.cfg); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getValidProducts() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_checkSuppiersDepending(t *testing.T) {
	type args struct {
		depending          []model.Product
		productToSuppliers map[model.Product]string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := checkSuppiersDepending(tt.args.depending, tt.args.productToSuppliers); got != tt.want {
				t.Errorf("checkSuppiersDepending() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_baseProcessByDate(t *testing.T) {
	type args struct {
		ctx context.Context
		fp  string
		p   MetricProcessor
		m   *Metric
		cfg *ProcessConfig
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseProcessByDate(tt.args.ctx, tt.args.fp, tt.args.p, tt.args.m, tt.args.cfg)
		})
	}
}

func Test_baseProcessByDateSql(t *testing.T) {
	type args struct {
		p   MetricProcessor
		m   *Metric
		cfg *ProcessConfig
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := baseProcessByDateSql(tt.args.p, tt.args.m, tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("baseProcessByDateSql() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("baseProcessByDateSql() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_removeStatData(t *testing.T) {
	type args struct {
		statModel model.StatModel
		metric    string
		tagId     int64
		date      *model_rule.Date
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := removeStatData(tt.args.statModel, tt.args.metric, tt.args.tagId, tt.args.date); (err != nil) != tt.wantErr {
				t.Errorf("removeStatData() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_baseProcessDayByDay(t *testing.T) {
	type args struct {
		ctx context.Context
		fp  string
		p   MetricProcessor
		m   *Metric
		cfg *ProcessConfig
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseProcessDayByDay(tt.args.ctx, tt.args.fp, tt.args.p, tt.args.m, tt.args.cfg)
		})
	}
}

func Test_baseProcessDayByDaySqls(t *testing.T) {
	type args struct {
		p   MetricProcessor
		m   *Metric
		cfg *ProcessConfig
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := baseProcessDayByDaySqls(tt.args.p, tt.args.m, tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("baseProcessDayByDaySqls() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("baseProcessDayByDaySqls() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_baseProcessSqls(t *testing.T) {
	type args struct {
		p   MetricProcessor
		ms  []*Metric
		cfg *ProcessConfig
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := baseProcessSqls(tt.args.p, tt.args.ms, tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("baseProcessSqls() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("baseProcessSqls() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_baseProcess(t *testing.T) {
	type args struct {
		ctx context.Context
		p   MetricProcessor
		ms  []*Metric
		cfg *ProcessConfig
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseProcess(tt.args.ctx, tt.args.p, tt.args.ms, tt.args.cfg)
		})
	}
}

func Test_mongoSelector(t *testing.T) {
	type args struct {
		campaignStat *AnalyseStat
		metric       string
	}
	tests := []struct {
		name string
		args args
		want bson.M
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mongoSelector(tt.args.campaignStat, tt.args.metric); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mongoSelector() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_exist(t *testing.T) {
	type args struct {
		value int
		slice []int
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := exist(tt.args.value, tt.args.slice); got != tt.want {
				t.Errorf("exist() = %v, want %v", got, tt.want)
			}
		})
	}
}
