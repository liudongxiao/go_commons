package reader

import (
	"io"
	"io/ioutil"
	"testing"
)

func TestMap(t *testing.T) {
	type args struct {
		f  io.ReadWriteCloser
		tf func(line string) string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"temp",
			args{TempFileWithDataFunc("", []byte("liu\ndong\nxiao\n"), func(line string) string {return line + "dong"}),
				func(line string) string {return line + "dong"
		}},
		false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer tt.args.f.Close()
			if err := Map(tt.args.f, tt.args.tf); (err != nil) != tt.wantErr {
				t.Errorf("Map() error = %v, wantErr %v", err, tt.wantErr)
			}
			buf, err := ioutil.ReadAll(tt.args.f)
			if err != nil {
				t.Error(err)
			} else {
				t.Log(string(buf))
			}
		})
	}
}
