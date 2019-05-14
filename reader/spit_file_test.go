package reader

import (
	"bytes"
	"dmp_web/go/commons/temp"
	"io"
	"reflect"
	"testing"
)

func TestSpilt(t *testing.T) {
	type args struct {
		in      io.ReadCloser
		maxLine int
	}
	b:=[]*bytes.Buffer{bytes.NewBufferString("hi\nhello\n"),bytes.NewBufferString("world\n")}
	bo:=make([]io.Reader,len(b))
	for i,v:=range b{
		bo[i]=v
	}
	tests := []struct {
		name string
		args args
		want []io.Reader
	}{
	 {"1",args{temp.OpenWithData("",[]byte("hi\nhello\nworld")),2},bo},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Spilt(tt.args.in, tt.args.maxLine); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Spilt() = %v, want %v", got, tt.want)
			}
		})
	}
}
