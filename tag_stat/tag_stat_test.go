package tag_stat

import (
	"context"
	"dmp_web/go/commons/db/hive"
	"dmp_web/go/model"
	"reflect"
	"testing"
)

func TestNewTagStat(t *testing.T) {
	type args struct {
		conn  hive.Cli
		tagId int64
	}
	tests := []struct {
		name string
		args args
		want *TagStat
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewTagStat(tt.args.conn, tt.args.tagId); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewTagStat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTagStat_getTagStage(t *testing.T) {
	tests := []struct {
		name    string
		t       *TagStat
		want    *model.TagStage
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.t.getTagStage()
			if (err != nil) != tt.wantErr {
				t.Errorf("TagStat.getTagStage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TagStat.getTagStage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTagStat_processTagCount(t *testing.T) {
	type args struct {
		ctx       context.Context
		dimension *Dimension
	}
	tests := []struct {
		name string
		t    *TagStat
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.t.processTagCount(tt.args.ctx, tt.args.dimension)
		})
	}
}

func TestTagStat_getTag(t *testing.T) {
	tests := []struct {
		name    string
		t       *TagStat
		want    *model.Tag
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.t.getTag()
			if (err != nil) != tt.wantErr {
				t.Errorf("TagStat.getTag() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TagStat.getTag() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTagStat_GetDimension(t *testing.T) {
	tests := []struct {
		name    string
		t       *TagStat
		want    *Dimension
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.t.GetDimension()
			if (err != nil) != tt.wantErr {
				t.Errorf("TagStat.GetDimension() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TagStat.GetDimension() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTagStat_Process(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		t       *TagStat
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.t.Process(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("TagStat.Process() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTagStat_dimensionSqls(t *testing.T) {
	tests := []struct {
		name    string
		t       *TagStat
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.t.dimensionSqls()
			if (err != nil) != tt.wantErr {
				t.Errorf("TagStat.dimensionSqls() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TagStat.dimensionSqls() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTagStat_GetReports(t *testing.T) {
	tests := []struct {
		name    string
		t       *TagStat
		want    []MetricProcessor
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.t.GetReports()
			if (err != nil) != tt.wantErr {
				t.Errorf("TagStat.GetReports() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TagStat.GetReports() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTagStat_GenetateReports(t *testing.T) {
	type args struct {
		ctx       context.Context
		processes []MetricProcessor
	}
	tests := []struct {
		name    string
		t       *TagStat
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.t.GenetateReports(tt.args.ctx, tt.args.processes); (err != nil) != tt.wantErr {
				t.Errorf("TagStat.GenetateReports() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTagStat_GetProcessRate(t *testing.T) {
	tests := []struct {
		name    string
		t       *TagStat
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.t.GetProcessRate(); (err != nil) != tt.wantErr {
				t.Errorf("TagStat.GetProcessRate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetStageTbls(t *testing.T) {
	type args struct {
		tags []*model.Tag
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
			if got := GetStageTbls(tt.args.tags...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetStageTbls() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMergeHQL(t *testing.T) {
	type args struct {
		tbls []string
		join bool
		id   string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := MergeHQL(tt.args.tbls, tt.args.join, tt.args.id)
			if got != tt.want {
				t.Errorf("MergeHQL() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("MergeHQL() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestGetId(t *testing.T) {
	type args struct {
		etype int
	}
	tests := []struct {
		name      string
		args      args
		wantField string
		wantErr   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotField, err := GetId(tt.args.etype)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetId() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotField != tt.wantField {
				t.Errorf("GetId() = %v, want %v", gotField, tt.wantField)
			}
		})
	}
}

func TestCreateMergeTbls(t *testing.T) {
	type args struct {
		ctx   context.Context
		join  bool
		hcli  hive.Cli
		etype int
		tags  []*model.Tag
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
			if err := CreateMergeTbls(tt.args.ctx, tt.args.join, tt.args.hcli, tt.args.etype, tt.args.tags...); (err != nil) != tt.wantErr {
				t.Errorf("CreateMergeTbls() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
