package pdmp

import "testing"

func TestExportToDsp(t *testing.T) {
	type args struct {
		cfg *Conf
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ExportToDsp(tt.args.cfg); (err != nil) != tt.wantErr {
				t.Errorf("ExportToDsp() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
