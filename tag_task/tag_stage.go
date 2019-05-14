package tag_task

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"dmp_web/go/commons/log"

	"dmp_web/go/commons/db/hive"
	"dmp_web/go/commons/errors"
	"dmp_web/go/commons/tag_stat"
	"dmp_web/go/model"
	"dmp_web/go/model/model_rule"
)

const (
	StageStateInit    = 0
	StageStateRunning = 1
	StageStateFinish  = 2
)

// 把所有查询变成 stage 和 depend 依赖
// 把一个树状结构的查询逻辑变成一个扁平的数组
// Depends 是当前 stage 对其他 stage 的依赖关系
// sql 就是 rule.HiveWhere 的查询条件
// Name 就是当前 stage 的名字，比如 stage-0 就是第一个任务
// Op 就是对所有 Depends 的 stage 做什么样的操作
type Stage struct {
	GenIdx  int
	Name    string
	Depends []string
	Sql     string

	State int // 表明运行的状态
	rule  *model_rule.Rule
}

func (s *Stage) IsLeafItem() bool {
	return len(s.Depends) == 0
}

type TagStage struct {
	prefix  string
	tag     *model.Tag
	date    *model_rule.Date
	counter int
	stages  []*Stage
	extend  *model_rule.ExtendConfig
	dbName  string
}

func NewTagStage(prefix string, tag *model.Tag, db string, extend *model_rule.ExtendConfig) *TagStage {
	date, _ := tag.GetDate()
	ts := &TagStage{
		prefix: prefix,
		tag:    tag,
		date:   date,
		dbName: db,
		extend: extend,
	}
	return ts
}

func (t *TagStage) Init() error {
	t.stages = nil
	rule := t.tag.Rule.Optimize(t.tag.TypeId == tag_stat.Mob)
	if err := t.AbstractTree(rule, nil); err != nil {
		return err
	}
	return t.saveStageTable()
}

func (t *TagStage) saveStageTable() error {
	lastStage := t.stages[len(t.stages)-1]
	ts := &model.TagStage{
		Id:  t.tag.Id,
		Tbl: lastStage.Name,
	}

	return ts.Save()

}

func (t *TagStage) findStage(name string, end int) *Stage {
	if end == -1 {
		end = len(t.stages)
	}
	for i := 0; i < end; i++ {
		if t.stages[i].Name == name {
			return t.stages[i]
		}
	}
	return nil
}

func (t *TagStage) isAllDependFinished(idx int) bool {
	for _, depname := range t.stages[idx].Depends {
		if dep := t.findStage(depname, idx); dep != nil {
			if dep.State != StageStateFinish {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

// -2: done, -1: wait, >=0: found
func (t *TagStage) pickNextRun() int {
	finishCount := 0
	for i := 0; i < len(t.stages); i++ {
		switch t.stages[i].State {
		case StageStateInit:
			// 检查依赖是否都已完成
			if t.isAllDependFinished(i) {
				return i
			}
		case StageStateRunning:
			// 该任务正在运行, 跳过
		case StageStateFinish:
			finishCount++
		}
	}
	if finishCount == len(t.stages) {
		return -2
	}
	return -1
}

// 执行tagStage　创建表
func (t *TagStage) Run(ctx context.Context, hcli hive.Cli) error {

	in := make(chan *Stage, 8)

	// 完成后调用, 当成功时, err为空
	errC := make(chan error, 1)
	onFinish := func(err error) {
		if err != nil {
			errC <- err
			return
		}
		if len(t.stages) == 0 {
			errC <- errors.Newf("no stage")
			return
		}

		if err := t.saveStageTable(); err != nil {
			errC <- err
			return
		}

		errC <- nil

	}

	// 启动一个goroutine, 当完成或者错误时会自动退出
	go t.kickLoop(ctx, hcli, in, onFinish)

	// 处理叶节点, 剩下的由leafItem 执行完成后调用 scheKick
	// 完成/错误后onFinish会被调用
	for _, stage := range t.stages {
		if stage.IsLeafItem() {
			in <- stage
		}
	}
	return <-errC
}

// 打印创建tagstage 的表语句
func (t *TagStage) QueryPlanDebug() string {
	buf := bytes.NewBuffer(nil)
	fmt.Fprintf(buf, "Tag: %v\n", t.tag.Id)
	buf.WriteString("Stage:\n")

	for _, stage := range t.stages {
		fmt.Fprintf(buf, "    %v: %s\n", stage.GenIdx, stage.Name)
	}

	buf.WriteString("STAGE DEPENDENCIES:\n")
	for _, itemStage := range t.stages {
		deps := make([]int, 0, len(itemStage.Depends))
		for _, name := range itemStage.Depends {
			s := t.findStage(name, -1)
			deps = append(deps, s.GenIdx)
		}
		fmt.Fprintf(buf, "    %v %v\n", itemStage.GenIdx, deps)
	}

	fmt.Fprintf(buf, "EXECUTE SEQUENCE:\n")
	for _, itemStage := range t.stages {
		fmt.Fprintf(buf, "    Execute: %v\n", itemStage.GenIdx)
		fmt.Fprintf(buf, "        SQL: %s\n", itemStage.Sql)
	}
	stageTable := buf.String()
	return stageTable
}

func (t *TagStage) GroupPlan() []string {
	sqls := make([]string, 0, len(t.stages))
	for _, itemStage := range t.stages {
		sqls = append(sqls, itemStage.Sql)
	}
	return sqls

}

// getStageName 给tagStage 表命名，　保证每次运行，包括重复运行的表名唯一
func (t *TagStage) getStageName(rule *model_rule.Rule) string {
	return t.dbName + "." + strings.Join([]string{
		fmt.Sprintf("%v_%v", t.prefix, t.tag.Id),

		// 供应商ID的列表
		fmt.Sprintf("%v_%v", "sid", t.tag.SupplierHash()),

		// visitorId 类型 , PC/移动
		fmt.Sprintf("%v_%v", "type", t.tag.TypeId),

		// 时间, 绝对时间, 不能是相对时间
		fmt.Sprintf("%v_%v", "date", t.date.TagNow()),

		// 添加关于key_policy里定义的SQL的hash
		// 方便以后修改的时候自动更新
		fmt.Sprintf("%v_%v", "sqlv", rule.HiveQueryHash()),

		// rule规则的hash
		// 如果用户做了修改, hash值会变
		fmt.Sprintf("%v_%v", "hash", rule.Hash()),
	}, "_")
}

// 抽象树，主要是把 树状 的嵌套条件，变成 array 状的扁平条件
// 其实可以参考 hive 的关键套件 http://www.antlr.org/
// 他是采用 stack 来实现计算
// stack 和递归是相通的
// 第一版比较草率，是一个递归实现的。
//func (t *TagStage) AbstractTree(rule *model_rule.Rule, parentStage *Stage) error {
//	//if rule.IsTagRef() {
//	//	return nil
//	//}
//	// 声明要给 stage 对象，完成后要把 stage counter += 1
//	t.counter++
//
//	var stage = &Stage{
//		GenIdx:  t.counter,
//		Name:    t.getStageName(rule),
//		Depends: make([]string, 0, 10),
//		rule:    rule,
//	}
//	// 如果是从其他的 stage 递归进入方法，就说明当前的 stage 是上一个 stage 的依赖
//	if parentStage != nil {
//		parentStage.Depends = append(parentStage.Depends, stage.Name)
//	}
//
//	buf := bytes.NewBuffer(nil)
//	buf.WriteString("CREATE TABLE IF NOT EXISTS ")
//	buf.WriteString(stage.Name)
//	buf.WriteString(" AS ")
//
//	if rule.IsLogicType() {
//		rules := rule.GetLogicRules()
//		for i := 0; i < len(rules); i++ {
//			if err := t.AbstractTree(&rules[i], stage); err != nil {
//				return err
//			}
//		}
//		// and: join depends
//		// or:  union depends
//		rule.MergeHQL(buf, stage.Depends)
//
//	} else if rule.IsTagRef() {
//		// 检查是不是rule是不是包含引用其它rule
//		ids := rule.Value.(model_rule.Int64sValue)
//		tags, err := model.RuleModel.FindByIds([]int64(ids))
//		if err != nil {
//			return fmt.Errorf("error on unmarshall rule%v: %v", ids, err)
//		}
//
//		var tmpStages []*Stage
//		for idx := range tags {
//			tag := &tags[idx]
//			ts, err := NewTagStage("rule", tag,stage)
//			if err != nil {
//				return err
//			}
//			tmpStages = append(tmpStages, ts.stages...)
//		}
//
//		switch rule.Op {
//		// 包含多个tag
//		case model_rule.OpIn:
//			if len(stage.Depends) == 1 {
//				// 引用只有一个标签, 把本stage改成引用标签的第一个
//				t.stages = append(t.stages, tmpStages...)
//				*stage = *tmpStages[len(tmpStages)-1]
//				log.Debugf("stage is %v", stage)
//				return nil
//
//			} else {
//				// 如果一下子引用多个标签
//				// 用 Or 条件把他们包起来
//				t.stages = append(t.stages, tmpStages...)
//				rule = &model_rule.Rule{
//					Key: model_rule.KeyOr,
//				}
//				rule.MergeHQL(buf, stage.Depends)
//			}
//		case model_rule.OpNin:
//			// TODO not implemented yet.
//			return fmt.Errorf("暂不支持排除人群规则")
//		default:
//		}
//	} else {
//		// 获取执行 SQL,关键
//		rule.HiveQueryEx(buf, rule.GenExtend(t.extend))
//	}
//
//	stage.sql = buf.String()
//	t.stages = append(t.stages, stage)
//	return nil
//}
func (t *TagStage) AbstractTree(rule *model_rule.Rule, parentStage *Stage) error {
	// 声明要给 stage 对象，完成后要把 stage counter += 1
	t.counter++
	var stage = &Stage{
		GenIdx:  t.counter,
		Name:    t.getStageName(rule),
		Depends: make([]string, 0, 10),
		rule:    rule,
	}

	// 如果是从其他的 stage 递归进入方法，就说明当前的 stage 是上一个 stage 的依赖
	if parentStage != nil {
		parentStage.Depends = append(parentStage.Depends, stage.Name)
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString("CREATE TABLE IF NOT EXISTS ")
	buf.WriteString(stage.Name)
	buf.WriteString(" AS ")

	if rule.IsLogicType() {
		rules := rule.GetLogicRules()
		for i := 0; i < len(rules); i++ {
			if err := t.AbstractTree(&rules[i], stage); err != nil {
				return err
			}
		}
		// and: join depends
		// or:  union depends
		rule.MergeHQL(buf, stage.Depends, t.tag.TypeId)
	} else if rule.IsTagRef() {
		// 检查是不是rule是不是包含引用其它rule
		ids := rule.Value.(model_rule.Int64sValue)
		tags, err := model.RuleModel.FindByIds([]int64(ids))
		if err != nil {
			return fmt.Errorf("error on unmarshall rule%v: %v", ids, err)
		}

		var tmpStages []*Stage
		for idx := range tags {
			tag := &tags[idx]
			extend, err := TagToExtendConfig(tag)
			if err != nil {
				return fmt.Errorf("err on extend%v: %v", ids, err)
			}
			ts := NewTagStage("rule", tag, t.dbName, extend)
			r := tag.Rule.Optimize(t.tag.TypeId == tag_stat.Mob)
			if err := ts.AbstractTree(r, stage); err != nil {
				return err
			}
			tmpStages = append(tmpStages, ts.stages...)
		}

		switch rule.Op {

		// 包含多个tag
		case model_rule.OpIn:
			if len(stage.Depends) == 1 {
				// 引用只有一个标签, 把本stage改成引用标签的第一个
				t.stages = append(t.stages, tmpStages...)
				*stage = *tmpStages[len(tmpStages)-1]
				return nil
			} else {
				// 如果一下子引用多个标签
				// 用 Or 条件把他们包起来
				t.stages = append(t.stages, tmpStages...)
				rule = &model_rule.Rule{
					Key: model_rule.KeyOr,
				}
				rule.MergeHQL(buf, stage.Depends, t.tag.TypeId)
			}
		case model_rule.OpNin:
			// not implemented yet.
			return fmt.Errorf("暂不支持排除人群规则")
		default:
		}
	} else {
		rule.HiveQueryEx(buf, rule.GenExtend(t.extend))
	}

	stage.Sql = buf.String()
	t.stages = append(t.stages, stage)
	return nil
}

// 按依赖执行各个stage
func (t *TagStage) kickLoop(ctx context.Context, hcli hive.Cli, in chan *Stage, onFinish func(error)) {
	var taskErr error
	var stoped int32
	hiveRetChan := make(chan *hiveReturn, 8)

	markError := func(s *Stage, err error) {
		atomic.StoreInt32(&stoped, 1)
		taskErr = fmt.Errorf("tag[%v] tagStage[%v] error: %v",
			t.tag.Id, s.Name, err,
		)
	}

	// 运行一个stage
	kick := func(s *Stage) error {
		// println("kick", s.GenIdx)
		s.State = StageStateRunning

		// 清理旧的表，主要是为了重跑的时候使用
		//tag_stat.DropTable(ctx, hcli, s.Name)
		//log.Debugf("tag[%v/%v/%v] execute: %v", t.tag.Id, s.GenIdx, s.Name, s.Sql)

		ret, err := hcli.ExecuteAsyncCtx(ctx, s.Sql)
		if err != nil {
			return errors.Wrap(errors.ErrTableCreat{s.Sql, err}, "")
		}

		log.Debug("Tag stage has finished process.")
		ret.RunOnFinish(func() {
			if atomic.LoadInt32(&stoped) == 1 {
				// 已经关闭了
				return
			}
			hiveRetChan <- &hiveReturn{
				stage: s,
				err:   ret.Err(),
			}
		})
		//// 调 runOnFinish 要先scan ret , 不然runOnFinish 里面的代码是不会执行的
		//if ret != nil {
		//	var number interface{}
		//	if ret.Next() {
		//		ret.Scan(&number)
		//	}
		//
		//	//log.Debugf(" ret: %+v, sql : %s ", ret, s.Sql)
		//} else {
		//	log.Debug("result is nil")
		//}
		return nil
	}

loop:
	for {
		select {

		// 从 run 进来
		case stage := <-in:
			if err := kick(stage); err != nil {
				markError(stage, err)
				break loop
			}

			// 接受hive的返回:
		case hiveRet := <-hiveRetChan:
			if hiveRet.err != nil {
				markError(hiveRet.stage, hiveRet.err)
				break loop
			}
			hiveRet.stage.State = StageStateFinish

			// 挑选下一个执行
			switch kickIdx := t.pickNextRun(); kickIdx {
			case -1:
				// 目前没有可运行的Stage
				// 由于是树结构, 所以由另一个节点的返回来触发他
			case -2: // 完成了
				break loop
			default:
				// 启动任务
				// 不走 in channel 是为了防止 channel 满时死锁
				if err := kick(t.stages[kickIdx]); err != nil {
					markError(t.stages[kickIdx], err)
					break loop
				}
			}
		}
	}

	if taskErr != nil {
		close(hiveRetChan)
		for range hiveRetChan {
		}
	}

	onFinish(taskErr)
}

type hiveReturn struct {
	stage *Stage
	err   error
}
