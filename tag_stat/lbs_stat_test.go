package tag_stat

import (
	"context"
	"dmp_web/go/commons/db/hive"
	"dmp_web/go/model"
	"reflect"
	"testing"
)

func TestNewLBsStat(t *testing.T) {
	type args struct {
		tag  *model.Tag
		hcli hive.Cli
	}
	tests := []struct {
		name string
		args args
		want *LbsStat
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewLBsStat(tt.args.tag, tt.args.hcli); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewLBsStat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLbsStat_GenerateGroup(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name          string
		lbsStat       *LbsStat
		args          args
		wantTableName string
		wantCount     int64
		wantErr       bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTableName, gotCount, err := tt.lbsStat.GenerateGroup(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("LbsStat.GenerateGroup() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotTableName != tt.wantTableName {
				t.Errorf("LbsStat.GenerateGroup() gotTableName = %v, want %v", gotTableName, tt.wantTableName)
			}
			if gotCount != tt.wantCount {
				t.Errorf("LbsStat.GenerateGroup() gotCount = %v, want %v", gotCount, tt.wantCount)
			}
		})
	}
}

func Test_getSql(t *testing.T) {
	type args struct {
		lat                float64
		lon                float64
		radius             int
		sdt                string
		edt                string
		tableName          string
		adUserConditionSql string
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
			if got := getSql(tt.args.lat, tt.args.lon, tt.args.radius, tt.args.sdt, tt.args.edt, tt.args.tableName, tt.args.adUserConditionSql); got != tt.want {
				t.Errorf("getSql() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_distance2Latitude(t *testing.T) {
	type args struct {
		distance int
	}
	tests := []struct {
		name string
		args args
		want float64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := distance2Latitude(tt.args.distance); got != tt.want {
				t.Errorf("distance2Latitude() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_distance2Longitude(t *testing.T) {
	type args struct {
		distance int
		lat      float64
		lon      float64
	}
	tests := []struct {
		name string
		args args
		want float64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := distance2Longitude(tt.args.distance, tt.args.lat, tt.args.lon); got != tt.want {
				t.Errorf("distance2Longitude() = %v, want %v", got, tt.want)
			}
		})
	}
}
