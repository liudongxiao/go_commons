package pdmp

import (
	"bufio"
	"dmp_web/go/commons/reader"
	"io"
	"testing"
)

//todo fix
func TestConf_GetReader(t *testing.T) {
	type fields struct {
		SupplierId      string
		Token           string
		UnixFiles       []string
		UserUploadFiles []string
		MobSubType      string
		DataType        int
		DataTypeStr     string
		PeopleId        int
		CreateUid       int
		LocalFIles      string
		LocalDir        string
		HdfsDir         string
		HdfsFile        string
		HiveTable       string
		Partition       string
		DspIds          string
		Label           string
		AdxId           string
		Op              string
	}

	reader.InitTest()
	tests := []struct {
		name    string
		fields  fields
		want    io.ReadCloser
		wantErr bool
	}{
		//{"hdfs_file", fields{HdfsFile: "/tmp/zhngyiming/user_tag_mapping.txt"}, nil, false},
		{"local_file", fields{LocalFIles: "/home/dong/te/upload_file_pdmp_test"}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Conf{
				SupplierId:      tt.fields.SupplierId,
				Token:           tt.fields.Token,
				UnixFiles:       tt.fields.UnixFiles,
				UserUploadFiles: tt.fields.UserUploadFiles,
				MobSubType:      tt.fields.MobSubType,
				DataType:        tt.fields.DataType,
				DataTypeStr:     tt.fields.DataTypeStr,
				PeopleId:        tt.fields.PeopleId,
				CreateUid:       tt.fields.CreateUid,
				LocalFIles:      tt.fields.LocalFIles,
				LocalDir:        tt.fields.LocalDir,
				HdfsFile:        tt.fields.HdfsFile,
				HiveTable:       tt.fields.HiveTable,
				Partition:       tt.fields.Partition,
				DspIds:          tt.fields.DspIds,
				Label:           tt.fields.Label,
				AdxId:           tt.fields.AdxId,
				Op:              tt.fields.Op,
			}
			i.Complete()
			got, err := i.GetReader()

			if (err != nil) != tt.wantErr {
				t.Errorf("Conf.GetReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				t.Fatalf("Conf.GetReader() = %+v", got)
			}
			bf := bufio.NewScanner(got)
			for bf.Scan() {
				t.Log(bf.Text())
			}
			if err = bf.Err(); bf != nil {
				t.Error(bf.Err())
			}
			got.Close()
		})
	}
}
