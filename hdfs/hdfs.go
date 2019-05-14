package hdfs

import (
	"dmp_web/go/conf"
	"fmt"
	"io"
	"sync"
)

var (
	hdfsOnce     sync.Once
	hdfsInstance *Client
)

func HDFS() (cli *Client) {
	hdfsOnce.Do(func() {
		cfg := conf.Get().Hdfs
		addr := fmt.Sprintf("%v:%v", cfg.Host, cfg.Port)
		var err error
		hdfsInstance, err = NewForUser(addr, cfg.User)
		if err != nil {
			panic(err)
		}
	})
	return hdfsInstance
}

func WriteTo(r io.Reader, dstPath string) (int64, error) {
	w, err := HDFS().Create(dstPath)
	if err != nil {
		return 0, err
	}
	defer w.Close()
	n, err := io.Copy(w, r)
	return n, err
}
