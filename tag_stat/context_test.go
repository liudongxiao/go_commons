package tag_stat

import (
	"context"
	"dmp_web/go/commons/db/hive"
	"dmp_web/go/model"
	"dmp_web/go/model/model_rule"
	"reflect"
	"testing"
)

func TestNewDimension(t *testing.T) {
	type args struct {
		hcli  hive.Cli
		tag   *model.Tag
		stage *model.TagStage
	}
	tests := []struct {
		name    string
		args    args
		want    *Dimension
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewDimension(tt.args.hcli, tt.args.tag, tt.args.stage)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewDimension() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewDimension() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDimension_GetTag(t *testing.T) {
	tests := []struct {
		name string
		d    *Dimension
		want *model.Tag
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.d.GetTag(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Dimension.GetTag() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDimension_TagId(t *testing.T) {
	tests := []struct {
		name string
		d    *Dimension
		want int64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.d.TagId(); got != tt.want {
				t.Errorf("Dimension.TagId() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDimension_Clean(t *testing.T) {
	type args struct {
		c      model.Cleaner
		metric string
		dt     string
	}
	tests := []struct {
		name    string
		d       *Dimension
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.d.Clean(tt.args.c, tt.args.metric, tt.args.dt); (err != nil) != tt.wantErr {
				t.Errorf("Dimension.Clean() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDimension_StatementByRange(t *testing.T) {
	type args struct {
		layout    string
		field     string
		date      *model_rule.Date
		suppliers []string
		name      string
	}
	tests := []struct {
		name    string
		d       *Dimension
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.d.StatementByRange(tt.args.layout, tt.args.field, tt.args.date, tt.args.suppliers, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("Dimension.StatementByRange() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Dimension.StatementByRange() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDimension_StatementsByDay(t *testing.T) {
	type args struct {
		layout         string
		field          string
		dts            []string
		suppliersGroup []string
		name           string
	}
	tests := []struct {
		name    string
		d       *Dimension
		args    args
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.d.StatementsByDay(tt.args.layout, tt.args.field, tt.args.dts, tt.args.suppliersGroup, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("Dimension.StatementsByDay() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Dimension.StatementsByDay() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDimension_StatementByRangeWithBaiduTable(t *testing.T) {
	type args struct {
		layout         string
		field          string
		date           *model_rule.Date
		suppliersGroup []string
		name           string
	}
	tests := []struct {
		name    string
		d       *Dimension
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.d.StatementByRangeWithBaiduTable(tt.args.layout, tt.args.field, tt.args.date, tt.args.suppliersGroup, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("Dimension.StatementByRangeWithBaiduTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Dimension.StatementByRangeWithBaiduTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDimension_HiveCtx(t *testing.T) {
	type args struct {
		ctx       context.Context
		statement string
		f         func(*hive.ExecuteResult)
	}
	tests := []struct {
		name    string
		d       *Dimension
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.d.HiveCtx(tt.args.ctx, tt.args.statement, tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("Dimension.HiveCtx() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDimension_CancelAll(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		d    *Dimension
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.d.CancelAll(tt.args.err)
		})
	}
}

func TestDimension_Progress(t *testing.T) {
	tests := []struct {
		name string
		d    *Dimension
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.d.Progress(); got != tt.want {
				t.Errorf("Dimension.Progress() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDimension_GetDate(t *testing.T) {
	tests := []struct {
		name string
		d    *Dimension
		want *model_rule.Date
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.d.GetDate(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Dimension.GetDate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDimension_baseProcess(t *testing.T) {
	type args struct {
		ctx context.Context
		p   MetricProcessor
		ret *hive.ExecuteResult
		f   func(*[]interface{})
	}
	tests := []struct {
		name    string
		d       *Dimension
		args    args
		want    int
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.d.baseProcess(tt.args.ctx, tt.args.p, tt.args.ret, tt.args.f)
			if (err != nil) != tt.wantErr {
				t.Errorf("Dimension.baseProcess() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Dimension.baseProcess() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_suppilerGroupValueToStr(t *testing.T) {
	type args struct {
		supplierGroup map[model.Product]string
		separator     string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := suppilerGroupValueToStr(tt.args.supplierGroup, tt.args.separator); got != tt.want {
				t.Errorf("suppilerGroupValueToStr() = %v, want %v", got, tt.want)
			}
		})
	}
}
