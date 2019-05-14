package tag_export

import (
	"bytes"
	"dmp_web/go/commons/errors"
	"dmp_web/go/commons/pdmp"
	"dmp_web/go/model/model_rule"
	"io"

	"dmp_web/go/commons/db/hive"
	"dmp_web/go/commons/env"
	"dmp_web/go/commons/log"
)

type ExportItem pdmp.ExportItem

func (e *ExportItem) Init() {

	if len(e.TagTypeStr) == 0 {
		if e.TagTypeInt == 1 {
			e.TagTypeStr = env.PCStr
		}
		if e.TagTypeInt == 2 {
			e.TagTypeStr = env.MobStr
		}
	}
	if e.TagTypeInt == 0 {
		if e.TagTypeStr == env.PCStr {
			e.TagTypeInt = 1
		} else if e.TagTypeStr == env.MobStr {
			e.TagTypeInt = 2
		}
	}
}

// 从hive中查询, 导出到远程
// 流式从hive中读取后, 写入到multipart里面
// http://web.gospel.biddingx.com/public/pdmp_data/
// dsp_masky/module/dmp_import/nsqsub/load.go#loadMPdmp
type TagExport struct {
	cfg  *pdmp.AuthConfig
	hive hive.Cli
}

func NewTagExport(cfg *pdmp.AuthConfig, hcli hive.Cli) *TagExport {
	return &TagExport{
		cfg:  cfg,
		hive: hcli,
	}
}

func (t *TagExport) GetQuery(item *ExportItem) string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString("SELECT ")
	buf.WriteString("cast(" + model_rule.VisitorKey + " as string)")
	buf.WriteString(" FROM ")
	buf.WriteString(item.Table)
	if item.PartitionName != "" {
		buf.WriteString(" where ")
		buf.WriteString(item.PartitionName)
		buf.WriteString(" = ")
		buf.WriteString(item.PartitionVaule)
	}
	// buf.WriteString(" LIMIT 10")
	return buf.String()
}

func (t *TagExport) GetResult(item *ExportItem) (io.ReadCloser, error) {
	query := t.GetQuery(item)
	log.Debugf("tag[%v] export Sql: %v", item.TagId, query)
	ret, err := t.hive.SubmitAsync(query)
	if err != nil {
		return nil, errors.New(err)
	}

	// 先阻塞直到数据准备好
	// 不然的话可能会发生在tcp连接写一半的时候阻塞
	log.Debugf("tag[%v] fetching hive result.", item.TagId)
	if !ret.Wait() {
		return nil, ret.Err()
	}

	// 获取到结果
	log.Debugf("tag[%v] result of hive is ready.", item.TagId)
	return NewHiveReader(ret, item.TagTypeInt, func(hi *HiveReader) {
		log.Debugf("tag[%v] uploading is done: sizes=%v, lines=%v",
			item.TagId, hi.TotalSize, hi.TotalLine)
	}), nil
}

func (t *TagExport) ExportAll(items []*ExportItem) error {
	pe, err := pdmp.NewPdmpExport(&pdmp.AuthConfig{
		Host:       t.cfg.Host,
		Token:      t.cfg.Token,
		SupplierId: t.cfg.SupplierId,
	})
	if err != nil {
		return err
	}
	for _, item := range items {
		eItem := (*pdmp.ExportItem)(item)
		err := pe.Export(eItem, func() (io.ReadCloser, error) {
			return t.GetResult(item)
		})
		if err != nil {
			return err
		}
	}
	return nil
}
