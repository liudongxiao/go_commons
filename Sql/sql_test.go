package Sql

import (
	"strings"
	"testing"
)

func TestGetCreateSql(t *testing.T) {
	type args struct {
		sql       string
		tableName string
	}
	tests := []struct {
		name               string
		args               args
		wantCreateTableSql string
		wantTable          string
	}{
		{"test1", args{"CREATE TABLE IF NOT EXISTS  dmpstage.461 AS",
			"dmpstage.461"},
			strings.Join(strings.Fields("CREATE TABLE IF NOT EXISTS  dmpstage.461  AS"), " "),
			"dmpstage.461"},
		{"test2", args{"select * from dmp.stage_114",
			"dmpstage.461"},
			strings.Join(strings.Fields("CREATE TABLE IF NOT EXISTS dmpstage.461  as select * from dmp.stage_114"), " "),
			"dmpstage.461"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCreateTableSql, gotTable := FormalCreateSql(tt.args.sql, tt.args.tableName)
			if gotCreateTableSql != tt.wantCreateTableSql {
				t.Errorf("FormalCreateSql() gotCreateTableSql = %v, want %v", gotCreateTableSql, tt.wantCreateTableSql)
			}
			if gotTable != tt.wantTable {
				t.Errorf("FormalCreateSql() gotTable = %v, want %v", gotTable, tt.wantTable)
			}
		})
	}
}

func TestGetTableName(t *testing.T) {
	type args struct {
		sql string
	}
	tests := []struct {
		name string
		args args
		want string
	}{{"test1", args{"CREATE TABLE IF NOT EXISTS dmpstage.461_sid AS"}, "dmpstage.461_sid"},
		{"test2", args{"CREATE TABLE dmpstage.461_sid AS"}, "dmpstage.461_sid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetTableName(tt.args.sql); got != tt.want {
				t.Errorf("GetTableName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckSafeSql(t *testing.T) {
	type args struct {
		sql string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"test1", args{"drop TABLE IF NOT EXISTS  dmpstage.461_sid  as"}, true},
		{"test1", args{"create TABLE IF NOT EXISTS  dmpstage.461_sid  as"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := CheckSafeSql(tt.args.sql); (err != nil) != tt.wantErr {
				t.Errorf("CheckSafeSql() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
