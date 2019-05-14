package pdmp

import (
	"bytes"
	"dmp_web/go/commons/errors"
	"dmp_web/go/commons/log"
	"dmp_web/go/model"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"

	mgo "gopkg.in/mgo.v2"
	bson "gopkg.in/mgo.v2/bson"
)

// http://web.gospel.biddingx.com/
// 接口代码
//   gospel/routers/pdmp/router.go
// 针对DSP提供的接口进行操作
//   1. 上传文件 (如果文件已经存在就不要上传了)
//   2. 导入数据
//   3. 检查进度 (同时反馈给前端)
//   4. 删除数据
type PdmpExport struct {
	cfg     *AuthConfig
	rootUrl url.URL
}

type ExportItem struct {
	TagId      int64
	Label      string
	DspUserIds []int // 支持导入到多个帐号
	//TagTypeStr 和 TagTypeInt 只是类型不同,
	// 表示导入的类型, pc=1 , mob=2
	TagTypeStr     string
	TagTypeInt     int
	Table          string
	PartitionName  string
	PartitionVaule string
	AuthId         []int64
}

func NewPdmpExport(cfg *AuthConfig) (*PdmpExport, error) {
	u, err := url.Parse(cfg.Host)
	if err != nil {
		return nil, err
	}
	return &PdmpExport{
		cfg:     cfg,
		rootUrl: *u,
	}, nil
}

type AuthConfig struct {
	Host       string
	Token      string
	SupplierId string
}

const (
	DefaultAdxId = "bdx"
)

var AuthCfg *AuthConfig

func init() {
	AuthCfg = &AuthConfig{Host: "http://api.gospel.biddingx.com/pdmp_data/upload/cookie",
		Token:      "23e43163e1173126256f7dda6472090c",
		SupplierId: "1008"}
}

func (p *PdmpExport) genUploadReq(records io.Reader, tagName, AdxId, tagType, op string) (*http.Request, error) {
	reqUrl := p.getURL("/upload/cookie", url.Values{
		"Id":       {p.cfg.SupplierId},
		"Label":    {tagName},
		"DataType": {tagType},
		"AdxId":    {AdxId},
		"OpType":   {op},
	})

	buf := bytes.NewBuffer(nil)
	mw := multipart.NewWriter(buf)

	// file 放到最后
	mw.CreateFormFile("DataFile", "file.txt")
	endReader := bytes.NewBufferString(fmt.Sprintf(
		"\r\n--%s--\r\n", mw.Boundary()))
	bodyReader := io.MultiReader(buf, records, endReader)

	req, err := http.NewRequest("POST", reqUrl.String(), bodyReader)
	if err != nil {
		return nil, errors.New(err)
	}
	req.Header.Set("Authorization", p.cfg.Token)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	return req, nil
}

func (p *PdmpExport) Export(item *ExportItem, getReader func() (io.ReadCloser, error)) error {
	taskTag := fmt.Sprintf("auth%v->tag[%v]", item.AuthId, item.TagId)

	job, err := model.PdmpJobModel.Get(item.TagId)
	if err != nil && err != mgo.ErrNotFound {
		return err
	}
	if job == nil {
		job = &model.PdmpJob{
			Id:    bson.NewObjectId(),
			TagId: item.TagId,
			Table: item.Table,
		}
	} else {
		// 如果Tag更新过, 则重新导入 或者不存在
		if job.Table != item.Table {
			log.Debugf("%v changed, %v -> %v", taskTag, job.Table, item.Table)
			job.State = model.PdmpJobInit
			job.Table = item.Table
			if err := job.Save(); err != nil {
				return err
			}
		}
	}

	for {
		switch job.State {
		case model.PdmpJobInit:
			log.Debugf("%v is uploading", taskTag)
			r, err := getReader()
			if err != nil {
				return err
			}
			jobId, err := p.Upload(item.Label, DefaultAdxId, item.TagTypeStr, "0", r)
			if err != nil {
				return err
			}
			job.JobId = jobId
			job.State = model.PdmpJobUploaded
			if err := job.Save(); err != nil {
				return err
			}
		case model.PdmpJobUploaded:
			log.Debugf("%v is uploaded, import to dsp", taskTag)
			// 上传完成后, 执行导入操作
			for _, dspUserId := range item.DspUserIds {
				if err := p.ImportToDsp(job.JobId, item.Label, strconv.Itoa(int(dspUserId))); err != nil {
					return err
				}
			}
			job.MarkImported()
		case model.PdmpJobImported:
			log.Debugf("%v is imported", taskTag)
			// 更新进度, 当完成后换成Finish状态
			return nil
		case model.PdmpJobFinished:
			log.Debugf("%v is finished", taskTag)
			return nil
		}
	}

	return nil
}

// 调用之前请检查文件是否存在
func (p *PdmpExport) Upload(tagName, AdxId, tagType string, op string, records io.Reader) (int, error) {
	req, err := p.genUploadReq(records, tagName, AdxId, tagType, op)
	if err != nil {
		return -1, err
	}
	resp, err := p.rpcCall(nil, req)
	if err != nil {
		return -1, err
	}
	return resp.Id, nil
}

func (p *PdmpExport) getURL(path string, values url.Values) *url.URL {
	u := p.rootUrl
	u.Path = "/pdmp_data" + path
	u.RawQuery = values.Encode()
	return &u
}

func (p *PdmpExport) rpcCall(ret interface{}, req *http.Request) (*Response, error) {
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf(string(body))
	}

	var realResp Response
	realResp.Result = ret
	if err := json.Unmarshal(body, &realResp); err != nil {
		return nil, fmt.Errorf("parse %v error: %v",
			strconv.Quote(string(body)), err)
	}
	if !realResp.Success {
		return nil, fmt.Errorf(realResp.Msg)
	}
	return &realResp, nil
}

func (p *PdmpExport) rpcGet(ret interface{}, u *url.URL) error {
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return err
	}
	_, err = p.rpcCall(ret, req)
	return err
}

type Response struct {
	Success bool        `json:"success"`
	Code    int         `json:"code"`
	Result  interface{} `json:"result"`
	Msg     string      `json:"msg"`
	Id      int         `json:"id"`
}

// 相当于执行了导入
func (p *PdmpExport) ImportToDsp(jobId int, tagName string, dspUserId string) error {
	reqUrl := p.getURL("/put/save", url.Values{
		"Label":     {tagName},
		"DspUserId": {dspUserId},
		"FileId":    {strconv.Itoa(int(jobId))},
	})
	req, err := http.NewRequest("POST", reqUrl.String(), nil)
	if err != nil {
		return err
	}
	if _, err := p.rpcCall(nil, req); err != nil {
		return err
	}
	return nil
}

type DmpImportProcess struct {
	Id          int64
	DspUserId   int
	CateName    string
	FileId      int64
	Size        int64
	ProcessSize int64
	Type        int
	OpType      int
	StartTime   int64
	EndTime     int64
}

// 获取进度 (导入是否完成)
func (p *PdmpExport) GetProgress(jobId int) (*DmpImportProcess, error) {
	reqUrl := p.getURL("/get/process", url.Values{
		"JobId": {strconv.Itoa(int(jobId))},
	})
	var resp DmpImportProcess
	if err := p.rpcGet(&resp, reqUrl); err != nil {
		return nil, err
	}
	return &resp, nil
}
