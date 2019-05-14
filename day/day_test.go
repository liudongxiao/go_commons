package day

import (
	"reflect"
	"testing"
)

func Test_getDayRange(t *testing.T) {
	type args struct {
		date string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{"test1",
			args{"20181212,20181214"},
			[]string{"20181214", "20181213", "20181212"},
			false},

		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetDayRange(tt.args.date)
			if (err != nil) != tt.wantErr {
				t.Errorf("getDayRange() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getDayRange() = %v, want %v", got, tt.want)
			}
		})
	}
}
