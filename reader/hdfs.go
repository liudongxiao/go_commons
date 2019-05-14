package reader

import (
	"io"
	"os"
	"path"

	"dmp_web/go/commons/env"

	"github.com/colinmarc/hdfs"
)

type hdfsFiles struct {
	HdfsPath   string
	HdfsFolder string
	LocalDir   string
}

var HdfsCli *HdfsClient

func Init() {
	HdfsCli = &HdfsClient{env.HdfsCli}

}

func InitTest()  {
	HadoopNameNode:="192.168.10.41:8020"
	hdfsCli, err := hdfs.New(HadoopNameNode)
	if err != nil {
		panic(err)
	}
	HdfsCli=&HdfsClient{hdfsCli}

}

type HdfsClient struct {
	*hdfs.Client
}

func (h *HdfsClient) Open(file string) (io.ReadCloser, error) {
	return h.Client.Open(file)
}

func (h *HdfsClient) OpenDir(dir string, rec bool) (io.ReadCloser, error) {

	fi, err := h.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(fi))
	for _, f := range fi {
		files = append(files, path.Join(dir, f.Name()))
	}
	rcs := make([]io.ReadCloser, 0, len(files))
	for _, f := range files {
		rc, err := h.Open(f)
		if err != nil {
			return nil, err
		}

		rcs = append(rcs, rc)
	}
	return MultiReadCloser(rcs...), nil
}

func (h *HdfsClient) OpenOneDir(dir string) (io.ReadCloser, error) {
	fi, err := h.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(fi))
	for _, f := range fi {
		files = append(files, path.Join(dir, f.Name()))
	}
	return h.OpenMultiFiles(files...)

}

func (h *HdfsClient) OpenMultiFiles(files ...string) (io.ReadCloser, error) {
	rcs := make([]io.ReadCloser, 0, len(files))
	for _, f := range files {
		rc, err := h.Open(f)
		if err != nil {
			return nil, err
		}

		rcs = append(rcs, rc)
	}
	return MultiReadCloser(rcs...), nil

}

func (h *HdfsClient) OpenRecreDir(dir string) (io.ReadCloser, error) {
	files := []string{}
	if err := h.Walk(dir, func(path string, f os.FileInfo, err error) error {
		if f.Mode().IsRegular() {
			files = append(files, path)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return h.OpenMultiFiles(files...)
}
