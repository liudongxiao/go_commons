package pdmp

import (
	"context"
	"dmp_web/go/commons/env"
	"dmp_web/go/commons/reader"
	"fmt"
	"io"
	"os"
	"strings"
)

//导入人群到hive 和 dsp 的配置
type Conf struct {
	SupplierId string
	Token      string
	UnixFiles  []string

	UserUploadFiles []string //完整路径名
	MobSubType      string
	DataType        int
	DataTypeStr     string //设备类型
	PeopleId        int    //人群Id
	CreateUid       int    //创建者id

	LocalFIles string // 本地文件
	localFIlesArr []string
	LocalDir   string

	HdfsFile   string //hdfs file
	hdfsFileArr []string
	HiveTable  string
	Partition  string

	DspIds string //导入dsp 的账户id, 支持多个,用逗号分隔
	Label  string //dsp人群标签
	AdxId  string

	Op string // 0 :add or 1 :delete

	//Test string // 测试环境
}

func NewImportDspConfig(files []string, mobSubType string, etypeStr string,
	etype int, peopleId int, dspIds, label, hdfsFile, op string) *Conf {
	c := &Conf{
		UserUploadFiles: files,
		MobSubType:      mobSubType,
		DataTypeStr:     etypeStr,
		DataType:        etype,
		PeopleId:        peopleId,
		DspIds:          dspIds,
		Label:           label,
		HdfsFile:        hdfsFile,
		Op:              op,
	}
	c.Complete()
	return c
}

//完成本地文件到hive表的操作，本方法会阻塞http请求，必须尽快返回
func (i *Conf) Import(ctx context.Context) error {
	return ExportToDsp(i)

}

func (i *Conf) checkGroupLineNum() (int64, error) {
	_, _, count, err := reader.MultiFileLines(i.DataType, i.UnixFiles...)
	if err != nil {
		return 0, err
	}
	return count, err
}

func (i *Conf) getFormalFiles(ctx context.Context) error {
	if len(i.UserUploadFiles) != len(i.UnixFiles) {
		return fmt.Errorf("UserUploadFiles length : %d , UnixFiles length : %d , not equal",
			len(i.UserUploadFiles), len(i.UnixFiles))
	}
	for index, f := range i.UserUploadFiles {
		r, err := os.Open(f)
		if err != nil {
			return err
		}
		w, err := os.Create(i.UnixFiles[index])
		if err != nil {
			return err
		}
		if err := reader.UnixLine(r, w, i.DataType, i.MobSubType); err != nil {
			return err
		}
	}
	return nil
}

//TODO unix file
func (i *Conf) GetReader() (io.ReadCloser, error) {
	wr := make([]io.ReadCloser, 0, len(i.UnixFiles))
		for _, f := range i.UserUploadFiles {
			fd, err := os.Open(f)
			if err != nil {
				return nil, err

			}
			stat, err := fd.Stat()
			if err != nil {
				return nil, err
			}
			w := &wrapReader{
				r:     fd,
				total: stat.Size(),
			}
			wr = append(wr, w)
		}
	if i.LocalDir != "" {
		r, err := reader.RecurDirReader(i.LocalDir)
		if err != nil {
			return nil, err
		}
		wr = append(wr, r)
	}

	if i.HdfsFile != "" {
		fs, err := reader.HdfsCli.Stat(i.HdfsFile)
		if err != nil {
			return nil, err
		}
		if fs.IsDir() {
			hdfsDir := i.HdfsFile
			rc, err := reader.HdfsCli.OpenRecreDir(hdfsDir)
			if err != nil {
				return nil, err
			}
			wr = append(wr, rc)
		} else {
			rc, err := reader.HdfsCli.OpenMultiFiles(i.hdfsFileArr...)
			if err != nil {
				return nil, err
			}
			wr = append(wr, rc)

		}
	}
	return reader.MultiReadCloser(wr...), nil
}

type Reader interface {
	GetReader() io.ReadCloser
}

func (i *Conf) Complete() {
	if i.LocalFIles != "" {
		i.UserUploadFiles = strings.Split(strings.ToLower(i.LocalFIles), ",")
	}
	if i.HdfsFile != "" {
		hdfsFileArr:=strings.Split(i.HdfsFile,",")
		for _,f:=range hdfsFileArr {
			HdfsFile := strings.TrimPrefix(strings.ToLower(f), "hdfs://")
			i.hdfsFileArr=append(i.hdfsFileArr,HdfsFile)
		}
	}
	if i.DataTypeStr != "" {
		if i.DataTypeStr == env.PCStr {
			i.DataType = env.PC
		} else if i.DataTypeStr == env.MobStr {
			i.DataType = env.Mob
		}
	}

	if i.SupplierId == "" {
		i.SupplierId = env.SupplierId
	}
	if i.Token == "" {
		i.Token = env.Token
	}

}

type LocalFiles struct {
	UnixFiles       []string //去重 和切分大小, 转成unix utf8 格式的文件
	UserUploadDir   string   // 用户上传文件夹, 递归读取文件下所有文件
	DirFiles        []string //  递归读取UserUploadDir 文件下所有文件
	UserUploadFiles []string // 用户上传的多个文件
	MobSubType      string   //mob 补全子类型
	DataType        int      //pc:1 mob:2
	DataTypeStr     string   // env.PCStr env.MobStr
}

func (l *LocalFiles) getFormalFiles() error {
	if len(l.UserUploadFiles) > 0 {
		for index, f := range l.UserUploadFiles {
			r, err := reader.MultiFileReader(f)
			if err != nil {
				return err
			}
			w, err := reader.MultiFileWriter(l.UnixFiles[index])
			if err != nil {
				return err
			}
			if err := reader.UnixLine(r, w, l.DataType, l.MobSubType); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *LocalFiles) getRecurDirFiles() error {
	files, err := reader.RecurDirFiles(l.UserUploadDir)
	if err != nil {
		return err
	}
	l.DirFiles = files
	return nil
}

func (l *LocalFiles) GetReader() (io.ReadCloser, error) {
	rc := make([]io.ReadCloser, 0, len(l.UnixFiles))
	if len(l.UnixFiles) != 0 {
		for _, f := range l.UnixFiles {
			fd, err := os.Open(f)
			if err != nil {
				return nil, err

			}
			stat, err := fd.Stat()
			if err != nil {
				return nil, err
			}

			w := &wrapReader{
				r:     fd,
				total: stat.Size(),
			}
			rc = append(rc, w)
		}
	}
	if len(l.DirFiles) > 0 {
		dc, err := reader.MultiFileReader(l.DirFiles...)
		if err != nil {
			return nil, err
		}
		if len(rc) > 0 {
			rc = append(rc, dc)

		}
	}
	return reader.MultiReadCloser(rc...), nil
}
