package tag_stat

import (
	"fmt"

	"dmp_web/go/model"

	"context"
	"dmp_web/go/commons/env"

	"dmp_web/go/commons/db/hive"
	"dmp_web/go/commons/log"
	"strings"
	"time"

	"dmp_web/go/commons/Sql"
	"dmp_web/go/commons/errors"

	"gopkg.in/bufio.v1"
)

//var HCli = env.HCli

const (
	TagStageTable = iota
	JoinBaiduTable
	DistinctJoinBiaudTable
)
const (
	PC  = env.PC
	Mob = env.Mob
)
const dspMob = Mob + 2

//　进度更新间隔
const TaskProcessFrequency = 10

// 1. 统计数量
type TagStat struct {
	tagId int64
	hcli  hive.Cli
}

func NewTagStat(conn hive.Cli, tagId int64) *TagStat {
	return &TagStat{
		tagId: tagId,
		hcli:  conn,
	}
}

func (t *TagStat) getTagStage() (*model.TagStage, error) {
	var tagStage *model.TagStage
	err := model.TagStageModel.FindId(t.tagId, &tagStage)
	if err != nil {
		return nil, err
	}
	return tagStage, nil
}

// 处理当前数量和昨日数量
func (t *TagStat) processTagCount(ctx context.Context, dimension *Dimension) {
	statement := "SELECT count(1) as cnt FROM " + dimension.stage.Tbl
	//env.SqlExist(env.Notify, ctx.Value(env.TagID).(string)+env.RedisALLKey, statement)
	//exist, err := env.SqlExist(env.Notify, ctx.Value(env.TagID).(string)+env.RedisDoneKey, statement)
	//if err == nil && exist {
	//	return
	//}
	err := dimension.HiveCtx(ctx, statement, func(ret *hive.ExecuteResult) {
		if !ret.NextCtx(ctx) {
			dimension.CancelAll(fmt.Errorf("error in get tag count: %v", ret.Err()))
			return
		}

		var count int64
		ret.Scan(&count)

		log.Debugf("tag[%v] cnt: %v", dimension.tag.Id, count)
		if err := dimension.tag.UpdateCount(int64(count)); err != nil {
			dimension.CancelAll(err)
			return
		}
	})
	if err != nil {
		dimension.CancelAll(err)
	}
}

func (t *TagStat) getTag() (*model.Tag, error) {
	var tag *model.Tag
	if err := model.TagModel.FindId(t.tagId, &tag); err != nil {
		return nil, err
	}
	return tag, nil
}

func (t *TagStat) GetDimension() (*Dimension, error) {
	tagStage, err := t.getTagStage()
	if err != nil {
		return nil, err
	}

	tag, err := t.getTag()
	if err != nil {
		return nil, err
	}

	dimension, err := NewDimension(t.hcli, tag, tagStage)
	if err != nil {
		return nil, err
	}
	return dimension, nil

}

func (t *TagStat) Process(ctx context.Context) error {
	mp, err := t.GetReports()
	if err != nil {
		return err
	}
	if err := t.GenetateReports(ctx, mp); err != nil {
		return err
	}
	return nil
}

func (t *TagStat) dimensionSqls() ([]string, error) {
	mp, err := t.GetReports()
	if err != nil {
		return nil, err
	}
	sqls := make([]string, 0, len(mp))

	for _, p := range mp {
		sql, err := p.ProcessSql()
		if err != nil {
			return nil, err
		}
		sqls = append(sqls, sql...)
	}

	return sqls, nil
}

//func (t *TagStat) ReportSqls() ([]string, error) {
//	tag, err := t.getTag()
//	if err != nil {
//		return nil, err
//	}
//	var sqls []string
//	sql, err := t.BaiduQuerys(tag)
//	if err != nil {
//		return nil, err
//	}
//	sql1, err := t.dimensionSqls()
//	if err != nil {
//		return nil, err
//	}
//	sqls = append(sqls, sql...)
//
//	sqls = append(sqls, sql1...)
//	return sqls, nil
//}

func (t *TagStat) GetReports() ([]MetricProcessor, error) {
	tagStage, err := t.getTagStage()
	if err != nil {
		return nil, err
	}

	tag, err := t.getTag()
	if err != nil {
		return nil, err
	}

	dimension, err := NewDimension(t.hcli, tag, tagStage)
	if err != nil {
		return nil, err
	}

	// 默认处理的报表 tag.PeopleSumary
	// 前端并没有区分，　暂时都放在默认报表这里
	var processes []MetricProcessor
	if tag.PeopleSumary {
		processes = append(processes,
			&TagStatSex{dimension},
			&TagStatAge{dimension},
			&TagStatHobby{dimension},
			&TagStatIndustry{dimension},
			&TagStatOs{dimension},
			&TagStatMobOs{dimension},
			&TagStatRegion{dimension},
			&TagStatSummary{dimension},
		)
	}

	// 广告分析
	if tag.AdvertisementSumary && tag.TypeId == PC {
		processes = append(processes, &TagStatAdSummary{dimension})
	}

	// 到站分析
	if tag.LandingPageSumary && tag.TypeId == PC {
		processes = append(processes,
			&TagStatDepth{dimension},
			&TagStatKeyword{dimension},
			&TagStatPage{dimension},
			&TagStatSource{dimension},
			&TagStatHotsite{dimension},
			&TagStatFrequency{dimension},
		)
	}

	// 花费分析
	if tag.CostingSumary {

	}
	return processes, nil

}

func (t *TagStat) GenetateReports(ctx context.Context, processes []MetricProcessor) error {
	dimension, err := t.GetDimension()
	if err != nil {
		return err
	}
	t.processTagCount(ctx, dimension)
	for _, p := range processes {
		p.Process(ctx)
	}
	return t.GetProcessRate()

}

func (t *TagStat) GetProcessRate() error {
	dimension, err := t.GetDimension()
	if err != nil {
		return err
	}
	tag, err := t.getTag()
	if err != nil {
		return err
	}

	for {
		<-time.Tick(time.Duration(time.Duration(TaskProcessFrequency) * time.Second))
		total, undone, done := dimension.hiveContext.Get()
		if undone != 0 {
			if total > 0 {
				processRate := int32(float32(done) / float32(total) * 100)
				tag.StateProcessRate = processRate
				tag.SaveState()
				log.Debugf("tag %d processRate %d ", tag.Id, processRate)
				log.Debugf("total:%d, undone:%d, done:%d", total, undone, done)
			}
		} else {
			tag.StateProcessRate = 100
			tag.SaveState()
			return nil
		}
	}
}

//func (t *TagStat) BaiduQuerys(tag *model.Tag) ([]string, error) {
//	var sqls []string
//	ts, err := t.getTagStage()
//	if err != nil {
//		return nil, err
//	}
//	tagStageTable:=ts.Tbl
//
//	countSql := Sql.CheckTableSql(tagStageTable)
//	baiduSqls, err := Sql.CreateJoinBaiduTableSqls(tbls[JoinBaiduTable], tbls[DistinctJoinBiaudTable],
//		tagStageTable, tag.TypeId)
//	if err != nil {
//		return nil, err
//	}
//	sqls = append(sqls, countSql)
//	sqls = append(sqls, baiduSqls...)
//	return sqls, nil
//
//}

//func (t *TagStat) Tbls() ([]string, error) {
//	ts := model.TagStage{Id: t.tagId}
//	tbls, err := ts.GetTb()
//	if err != nil {
//		return nil, nil
//	}
//	tbl := tbls[TagStageTable]
//	baiduJoinTable := fmt.Sprintf("%s_baidu_wide_table", tbl)
//	distinctBaiduJoinTable := fmt.Sprintf("%s_distinct_baidu_wide_table", tbl)
//
//	retTables := []string{tbl, baiduJoinTable, distinctBaiduJoinTable}
//
//	ts.Tbl = retTables
//	if err := ts.Save(); err != nil {
//		return nil, err
//	}
//	return retTables, nil
//}

//func (t *TagStat) CreateReportTempTables(ctx context.Context, tag *model.Tag) error {
//
//	sqls, err := t.BaiduQuerys(tag)
//	if err != nil {
//		return err
//	}
//	for _, sql := range sqls {
//		if _, err := t.hcli.ExecuteSyncCtx(ctx, sql); err != nil {
//			return err
//		}
//	}
//	return nil
//}

func GetStageTbls(tags ...*model.Tag) []string {
	var err error
	if len(tags) == 0 {
		return nil
	}

	var tagStageTables []string
	var tagStageModel *model.TagStage

	for _, tag := range tags {
		err = model.TagStageModel.FindId(tag.Id, &tagStageModel)
		if err != nil {
			return nil
		}
		tagStageTables = append(tagStageTables, tagStageModel.Tbl)
	}
	return tagStageTables

}

func MergeHQL(tbls []string, join bool, id string) (string, string) {
	if len(tbls) == 0 {
		return "", ""
	}
	tbl := strings.Join(tbls, ",")
	var b bufio.Buffer
	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(tbl)
	b.WriteString(" AS ")
	if join == true {
		b.WriteString("SELECT t1.$(id) FROM ")
		b.WriteString(tbls[0])
		b.WriteString(" AS t1")
		for i := 1; i < len(tbls); i++ {
			b.WriteString(" JOIN ")
			b.WriteString(tbls[i])
			b.WriteString(fmt.Sprintf(
				" AS t%v ON t1.$(id)=t%v.$(id) ", i+1, i+1))
		}
	} else {
		for i := 0; i < len(tbls); i++ {
			b.WriteString("SELECT $(id) FROM ")
			b.WriteString(tbls[i])
			if i == len(tbls)-1 {
				break
			}
			b.WriteString(" UNION ALL ")
		}
	}
	sql := b.String()
	sql = strings.Replace(sql, "$(Id)", id, -1)
	return tbl, sql
}
func GetId(etype int) (field string, err error) {
	if etype == PC {
		return "visitor_id", nil
	} else if etype == Mob {
		return "did", nil
	} else {
		return "", errors.Wrap(errors.ErrType{etype}, "")
	}

}

func CreateMergeTbls(ctx context.Context, join bool, hcli hive.Cli, etype int, tags ...*model.Tag) error {
	field, err := GetId(etype)
	if err != nil {
		return err
	}
	tbl, mergeSql := MergeHQL(GetStageTbls(tags...), join, field)
	if mergeSql == "" {
		return errors.Newf("no tags, so don't create tbl")
	}

	ret, err := hcli.ExecuteSyncCtx(ctx, mergeSql)
	if err != nil {
		return err
	}
	if ret == nil {
		return nil
	}
	num, err := Sql.CheckTableResultNumber(ctx, tbl)
	if err != nil {
		return err
	}
	if num == 0 {
		return errors.Newf("ZERO record")
	}
	return nil
}
