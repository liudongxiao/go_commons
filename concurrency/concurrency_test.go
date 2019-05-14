package concurrency

import (
	"bytes"
	"dmp_web/go/commons/temp"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestRunRet(t *testing.T) {
	type args struct {
		fi  io.Reader
		num int
		f   func(line interface{}) interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantFo  string
		wantErr bool
	}{
		{"1", args{strings.NewReader(lines), 3, func(line interface{}) interface{} { return line }}, "hihelloworld", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fo := &bytes.Buffer{}
			if err := RunRet(tt.args.fi, fo, tt.args.num, tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("RunRet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotFo := fo.String(); gotFo != tt.wantFo {
				t.Errorf("RunRet() = %v, want %v", gotFo, tt.wantFo)
			}
		})
	}
}

func TestRunFileRet(t *testing.T) {
	fi, fo := temp.CetFiFoName([]byte(lines))
	type args struct {
		fin  string
		fout string
		num  int
		f    func(line interface{}) interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"1", args{fi, fo, 3, func(line interface{}) interface{} { return line }}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := RunFileRet(tt.args.fin, tt.args.fout, tt.args.num, tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("RunFileRet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRun(t *testing.T) {
	type args struct {
		fi  io.Reader
		num int
		f   func(line interface{})
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"1", args{strings.NewReader("liu\ndong\nxiao"), 3, func(line interface{}) { fmt.Println(line) }}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Run(tt.args.fi, tt.args.num, tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("Run() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

const lines = "hi\nhello\nworld"

func TestRunFile(t *testing.T) {
	fi, _ := temp.CetFiFoName([]byte(lines))
	type args struct {
		fin string
		num int
		f   func(line interface{})
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"1", args{fi, 3, func(line interface{}) { fmt.Println(line) }}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := RunFile(tt.args.fin, tt.args.num, tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("RunFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
