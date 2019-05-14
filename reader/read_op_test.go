package reader

import (
	"dmp_web/go/commons/env"
	"dmp_web/go/commons/temp"
	"io"
	"strings"
	"testing"
)

func TestIsIdfa(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"true", args{"2589F89C-A93C-479A-B257_80B035D8C281"}, true},
		{"wrong", args{"2589F89C-A93C-479A-B257_80B035D8C281&&"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsIdfa(tt.args.str); got != tt.want {
				t.Errorf("IsIdfa() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFileLines(t *testing.T) {
	type args struct {
		etype int
		r     io.Reader
	}
	tests := []struct {
		name          string
		args          args
		wantAllLine   int64
		wantFalseLine int64
		wantTrueLine  int64
		wantErr       bool
	}{
		{"mob", args{env.Mob, strings.NewReader(lines)}, 1, 0, 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAllLine, gotFalseLine, gotTrueLine, err := FileLines(tt.args.etype, tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileLines() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotAllLine != tt.wantAllLine {
				t.Errorf("FileLines() gotAllLine = %v, want %v", gotAllLine, tt.wantAllLine)
			}
			if gotFalseLine != tt.wantFalseLine {
				t.Errorf("FileLines() gotFalseLine = %v, want %v", gotFalseLine, tt.wantFalseLine)
			}
			if gotTrueLine != tt.wantTrueLine {
				t.Errorf("FileLines() gotTrueLine = %v, want %v", gotTrueLine, tt.wantTrueLine)
			}
		})
	}
}

const lines = "idfa 478AA328-6844-4502-965E-3AAA78C12CD2\n"
const lines_no_type = "478AA328-6844-4502-965E-3AAA78C12CD2\n"

func TestUnixLine(t *testing.T) {
	fi,fo:=temp.CetFiFo([]byte(lines))
	fi_1,fo_2:=temp.CetFiFo([]byte(lines_no_type))

	type args struct {
		r             io.ReadCloser
		w             io.WriteCloser
		equipmentType int
		mobSubType    string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"no_auto_complete",args{fi,fo,env.Mob,"idfa"},false},
		{"auto_complete",args{fi_1,fo_2,env.Mob,"idfa"},false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := UnixLine(tt.args.r, tt.args.w, tt.args.equipmentType, tt.args.mobSubType); (err != nil) != tt.wantErr {
				t.Errorf("UnixLine() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
