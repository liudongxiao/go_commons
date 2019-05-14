package tag_export

import (
	"bytes"
	"dmp_web/go/commons/errors"
	"dmp_web/go/model"
	"io"

	"dmp_web/go/commons/db/hive"
)

// 将hive的结果封装成reader
type HiveReader struct {
	buf       *bytes.Buffer
	ret       *hive.ExecuteResult
	TotalLine int
	TotalSize int64
	mode      int
	onFinish  func(*HiveReader)
}

// mode 参照 model.
func NewHiveReader(ret *hive.ExecuteResult, mode int, onFinish func(*HiveReader)) *HiveReader {
	return &HiveReader{
		buf:      bytes.NewBuffer(nil),
		ret:      ret,
		mode:     mode,
		onFinish: onFinish,
	}
}

//下游服务要求每行的格式必须为 string string
func (h *HiveReader) fillMob() error {
	h.buf.Reset()
	var typ string
	var visitorId string
	if h.ret.NextPage() {
		for h.ret.NextInPage() {
			h.TotalLine++
			h.ret.Scan(&typ, &visitorId)
			if typ == "" {
				continue
			}
			h.buf.WriteString(typ)
			h.buf.WriteByte('\t')
			h.buf.WriteString(visitorId)
			h.buf.WriteByte('\n')
		}
	}
	h.TotalSize += int64(h.buf.Len())
	return h.ret.Err()
}

func (h *HiveReader) fillPC() error {
	h.buf.Reset()
	var visitorId string
	//var typ string
	if h.ret.Err() != nil {
		return h.ret.Err()
	}
	if h.ret.NextPage() {
		for h.ret.NextInPage() {
			h.TotalLine++
			h.ret.Scan(&visitorId)
			h.buf.WriteString(visitorId)
			h.buf.WriteByte('\n')
		}
	}
	h.TotalSize += int64(h.buf.Len())
	return nil
}

func (h *HiveReader) Read(b []byte) (int, error) {
	if h.buf.Len() == 0 {
		var err error
		if h.mode == model.TagTypePc {
			err = h.fillPC()
		} else if h.mode == model.TagTypeMob {
			err = h.fillMob()
		}
		if err != nil {
			return 0, errors.New(err)
		}
	}

	if h.buf.Len() == 0 {
		if h.onFinish != nil {
			h.onFinish(h)
		}
		return 0, io.EOF
	}
	n, err := h.buf.Read(b)
	if err != nil {
		return n, err
	}
	return n, nil
}

func (h *HiveReader) Close() error {
	return nil
}
