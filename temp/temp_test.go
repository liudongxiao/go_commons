package temp

import (
	"io"
	"reflect"
	"testing"

	"github.com/spf13/afero"
)

func TestOpenWithData(t *testing.T) {
	d1 := []byte("hello\nworld\n")
	d2:=[]byte("hi")
	type args struct {
		name string
		data []byte
	}
	tests := []struct {
		name string
		args args
		want afero.File
	}{

		{"t1", args{"t1", d1}, nil},
		{"t2", args{"t2", d2}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := OpenWithData(tt.args.name, tt.args.data)
			out := make([]byte,len(tt.args.data))
			_, err := io.ReadFull(got, out)
			if err != nil && err!=io.EOF {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(tt.args.data, out) {
				t.Errorf("OpenWithData()'s d1 = %v, want %v", out, tt.args.data)
			}
		})
	}
}
