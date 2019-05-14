package pdmp

import (
	"dmp_web/go/commons/errors"
	"fmt"
	"io"
	"strconv"
)

type wrapReader struct {
	r       io.ReadCloser
	total   int64
	current int64
}

func (r *wrapReader) Read(b []byte) (int, error) {
	n, err := r.r.Read(b)
	r.refresh(n)
	return n, err
}

func (r *wrapReader) refresh(n int) {
	r.current += int64(n)
	total := ""
	if r.total > 0 {
		total = fmt.Sprintf("/%v", r.total)
	}
	//log.Debugf(fmt.Sprintf("\r%v%v\033[K", r.current, total))
	fmt.Printf(fmt.Sprintf("\r%v%v\033[K", r.current, total))
}

func (r *wrapReader) Close() error {
	return r.r.Close()
}

func ExportToDsp(cfg *Conf) error {
	pe, err := NewPdmpExport(&AuthConfig{
		Host:       "http://api.gospel.biddingx.com/pdmp_data/upload/cookie",
		Token:      cfg.Token,
		SupplierId: cfg.SupplierId,
	})
	if err != nil {
		return errors.New(err)
	}
	r, err := cfg.GetReader()
	if err != nil {
		return errors.New(err)
	}
	defer r.Close()

	id, err := pe.Upload(cfg.Label, cfg.AdxId, strconv.Itoa(cfg.DataType), cfg.Op, r)
	if err != nil {
		return errors.New(err)
	}
	if err := pe.ImportToDsp(id, cfg.Label, cfg.DspIds); err != nil {
		return errors.New(err)
	}

	return nil

}
