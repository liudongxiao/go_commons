package crontab

import (
	"context"
	"dmp_web/go/commons/log"
	"dmp_web/go/model"
	"fmt"
	"time"
)

type CrontabItem struct {
	Daytime     time.Time
	NextExecute time.Time
	Execute     func() error
}

type Executer interface {
	// 当时间到达时会执行该函数
	// 返回错误时, 表示丢弃该计划任务(不自动添加)
	OnCrontab(ctx context.Context, targetId int64) error
}

type Crontab struct {
	executer []Executer
	tq       *TimeQueue
}

func NewCrontab() *Crontab {
	c := &Crontab{
		tq: NewTimeQueue(),
	}
	return c
}

func (c *Crontab) GetExecuter(typ int) Executer {
	if len(c.executer) > typ {
		return c.executer[typ]
	}
	return nil
}

func (c *Crontab) getToday() time.Time {
	now := time.Now()
	_, offset := now.Zone()
	return now.Round(24 * time.Hour).Add(-time.Duration(offset) * time.Second)
}

func (c *Crontab) loop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			out, _ := c.tq.Pop(now)
			if out == nil {
				continue
			}
			crontab := out.(*model.Crontab)
			executer := c.GetExecuter(crontab.TargetType)
			if executer == nil {
				// drop this crontab
				continue
			}
			executeErr := executer.OnCrontab(ctx, crontab.TargetId)
			if executeErr != nil {
				// send back to queue
				// TODO: 删除
				continue
			}

			crontab.NextTime = c.getToday().Add(crontab.Period)
			if err := crontab.Save(); err != nil {
				log.Error(err)
				continue
			}
			c.set(crontab)
		}
	}
}

func (c *Crontab) Init(ctx context.Context) error {
	crontabs, err := model.CrontabModel.All(nil)
	if err != nil {
		return err
	}
	for _, task := range crontabs {
		c.set(task)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		go c.loop(ctx)
		return nil
	}
}

func (c *Crontab) RegisterExecuter(typ int, e Executer) {
	if len(c.executer) <= typ {
		executers := make([]Executer, typ+1)
		copy(executers, c.executer)
		c.executer = executers
	}
	if c.executer[typ] != nil {
		panic(fmt.Sprintf("type of executer is in used: %v", typ))
	}
	c.executer[typ] = e
}

func (c *Crontab) set(crontab *model.Crontab) {
	c.tq.Upsert(crontab, crontab.NextTime)
}

// 添加计划任务, 马上执行
func (c *Crontab) SetNow(targetId int64, period time.Duration, typ int) error {
	return c.Set(targetId, time.Now(), period, typ)
}

// 添加计划任务, 下次执行
func (c *Crontab) SetNext(targetId int64, period time.Duration, typ int) error {
	var start time.Time
	return c.Set(targetId, start, period, typ)
}

// daytime: 每天执行的时间刻度, 范围 >0, 不
func (c *Crontab) Set(targetId int64, start time.Time, period time.Duration, typ int) error {
	// 计划任务只能存在一个
	task, err := model.CrontabModel.Set(targetId, start, period, typ)
	if err != nil {
		return err
	}

	c.set(task)
	return nil
}
