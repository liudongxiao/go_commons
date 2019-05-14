package errors

import (
	"dmp_web/go/commons/log"
	"fmt"
	"runtime/debug"
	"strings"
)

type ErrType struct {
	Etype interface{}
}

// Error returns a stringified error
func (e ErrType) Error() string {
	return fmt.Sprintf(" %s 类型错误", e.Etype)
}

type ErrTableZero struct {
	Table string
}

// Error returns a stringified error
func (e ErrTableZero) Error() string {
	return fmt.Sprintf(" %s 表数量为 zero ", e.Table)
}

type ErrTableRetNil struct {
	Table string
}

func (e ErrTableRetNil) Error() string {
	return fmt.Sprintf(" %s 表查询返回为nil ", e.Table)
}

type ErrGroupZero struct {
	Group string
}

// Error returns a stringified error
func (e ErrGroupZero) Error() string {
	return fmt.Sprintf(" %s 人群包数量为 zero ", e.Group)
}

type ErrTagZero struct {
	Tag int64
}

var ErrNil = Newf("nil error")

// Error returns a stringified error
func (e ErrTagZero) Error() string {
	return fmt.Sprintf(" %d 人群包数量为 zero", e.Tag)
}

type ErrTableCreat struct {
	Table string
	Err   error
}

// Error returns a stringified error
func (e ErrTableCreat) Error() string {
	return fmt.Sprintf(" %s 表创建失败, 失败原因: %s", e.Table, e.Err.Error())
}

type ErrMgoFind struct {
	Table interface{}
	Err   error
}

// Error returns a stringified error
func (e ErrMgoFind) Error() string {
	return fmt.Sprintf(" %v 表查找失败, 失败原因: %s", e.Table, e.Err.Error())
}

type ErrDay struct {
	Date string
}

func (e ErrDay) Error() string {
	return fmt.Sprintf(" 日期格式错误  %s", e.Date)

}

type ErrMgoInsert struct {
	Dimension string
}

func (e ErrMgoInsert) Error() string {
	return fmt.Sprintf("   %v 插入到 mongo 失败", e.Dimension)

}

type ErrSql struct {
	Sql string
}

func (e ErrSql) Error() string {
	return fmt.Sprintf("%s 不是有效sql", e.Sql)
}

type ErrSuppiler struct {
	TagId int64
}

func (e ErrSuppiler) Error() string {
	return fmt.Sprintf("tag %d 没有有效的供应商账号", e.TagId)
}

//　每个gorutine 都要加入handlePanic 函数，防止panic 程序挂了
func HandlePanic() {
	if r := recover(); r != nil {
		err := Newf("panic: %+v\nstack:%s", r, string(debug.Stack()))
		log.Error(err)
	}
}

func ReRunErr(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(ErrTableZero); ok {
		return false
	}
	if _, ok := err.(ErrGroupZero); ok {
		return false
	}
	if _, ok := err.(ErrTagZero); ok {
		return false
	}
	if strings.Contains(err.Error(), "zero") {
		return false
	}

	return true
}
