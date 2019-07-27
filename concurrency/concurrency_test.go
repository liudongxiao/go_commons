package concurrency

import (
	"bytes"
	"dmp_web/go/commons/temp"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestRunRet(t *testing.T) {
	InitTest(10000)
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
		{"1", args{strings.NewReader(string(lines)), 3, func(line interface{}) interface{} { return line.(string) + "\n" }}, string(lines), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fo := &bytes.Buffer{}
			if err := RunRet(tt.args.fi, fo, tt.args.num, tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("RunRet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotFo := fo.String(); len(gotFo) != len(tt.wantFo) {
				t.Errorf("RunRet() = %v, want %v", gotFo, tt.wantFo)
			}
			t.Logf("count is %d", cnt)
		})
	}
}

func TestRunFileRet(t *testing.T) {
	InitTest(100000)
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
	InitTest(10)
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
		//{"1", args{strings.NewReader(string(lines)), 1, func(line interface{}) { fib(len(line.(string))) }}, false},
		{"1", args{strings.NewReader(string(lines)), 4, func(line interface{}) { fib(len(line.(string))) }}, false},
		//{"1", args{strings.NewReader(string(lines)), 8, func(line interface{}) { fib(len(line.(string))) }}, false},
		//{"1", args{strings.NewReader(string(lines)), 12, func(line interface{}) { fib(len(line.(string))) }}, false},
		//{"1", args{strings.NewReader(string(lines)), 16, func(line interface{}) { fib(len(line.(string))) }}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Now()
			if err := Run(tt.args.fi, tt.args.num, tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("Run() error = %v, wantErr %v", err, tt.wantErr)
			}
			last := time.Now().Sub(start).Seconds()
			fmt.Println(last)
		})
		t.Logf("count is %d", cnt)
	}
}

func TestRunRet1(t *testing.T) {
	InitTest(10)
	type args struct {
		fi  io.Reader
		num int
		f   func(line interface{}) interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"1", args{strings.NewReader(string(lines)), 1, func(line interface{}) interface{} { return fib(len(line.(string))) }}, false},
		//{"1", args{strings.NewReader(string(lines)), 4, func(line interface{})interface{} {return fib(len(line.(string)))}}, false},
		//{"1", args{strings.NewReader(string(lines)), 8, func(line interface{})interface{} {return fib(len(line.(string)))}}, false},
		//{"1", args{strings.NewReader(string(lines)), 12, func(line interface{})interface{} {return fib(len(line.(string)))}}, false},
		//{"1", args{strings.NewReader(string(lines)), 16, func(line interface{})interface{} {return fib(len(line.(string)))}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fo := &bytes.Buffer{}
			start := time.Now()
			if err := RunRet(tt.args.fi, fo, tt.args.num, tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("Run() error = %v, wantErr %v", err, tt.wantErr)
			}
			last := time.Now().Sub(start).Seconds()
			fmt.Println(last)

			//if gotFo := fo.String(); len(gotFo) != len(tt.wantFo) {
			//	t.Errorf("RunRet() = %v, want %v", gotFo, tt.wantFo)
			//}
			t.Logf("count is %d", cnt)
		})

	}
}

const line = "hi\nhello\nworld\n11111111111111111111111111111111111111111\n"

var lines []byte

func InitTest(n int) {
	_lines := bytes.NewBufferString(line)
	for i := 0; i < n; i++ {
		_lines.WriteString(line)
	}
	lines = _lines.Bytes()

}

func TestRunFile(t *testing.T) {
	InitTest(10000)
	type args struct {
		fin string
		num int
		f   func(line interface{})
	}
	fi, _ := temp.CetFiFoName(lines)
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"1", args{fi, runtime.NumCPU(), func(line interface{}) {}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := RunFile(tt.args.fin, tt.args.num, tt.args.f); (err != nil) != tt.wantErr {
				t.Errorf("RunFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
//func BenchmarkRunRet(b *testing.B) {
//	InitTest(10)
//	type args struct {
//		fi  io.Reader
//		num int
//		f   func(line interface{}) interface{}
//	}
//	tests := []struct {
//		name    string
//		args    args
//		wantFo  string
//		wantErr bool
//	}{
//		{"1", args{strings.NewReader(string(lines)), 3, func(line interface{}) interface{} { return line.(string) + "\n" }}, string(lines), false},
//	}
//	for _, tt := range tests {
//		for n := 0; n < b.N; n++ {
//			b.Run(tt.name, func(b *testing.B) {
//				fo := &bytes.Buffer{}
//				if err := RunRet(tt.args.fi, fo, tt.args.num, tt.args.f); (err != nil) != tt.wantErr {
//					b.Errorf("RunRet() error = %v, wantErr %v", err, tt.wantErr)
//					return
//				}
//				if gotFo := fo.String(); len(gotFo) != len(tt.wantFo) {
//					b.Errorf("RunRet() = %v, want %v", gotFo, tt.wantFo)
//				}
//			})
//		}
//	}
//
//}



func fib(n int) int {
	if n < 2 {
		return n
	}
	return fib(n-1) + fib(n-2)
}
