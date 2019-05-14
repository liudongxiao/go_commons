package reader

import (
	"io"
	"testing"
)

func TestTempFileNew(t *testing.T) {
	type args struct {
		name string
		data []byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{

		 {"test_nil",args{"",[]byte{'1'}},false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := TempFileFunc(tt.args.name, tt.args.data, func(r io.ReadWriteCloser ) error {
			return  nil
			}); (err != nil) != tt.wantErr {
				t.Errorf("TempFileNew() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

