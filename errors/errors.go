package errors

import (
	"fmt"
	"path/filepath"
	"runtime"

	pkgErrors "github.com/pkg/errors"
)

func getName(depth int) string {
	pc, _, n, ok := runtime.Caller(1 + depth)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%v-%v",
		filepath.Base(runtime.FuncForPC(pc).Name()), n)
}

func getFormat(objs []interface{}) string {
	if len(objs) == 0 {
		return ""
	}
	s := fmt.Sprint(objs...)
	return " " + s
}

func Newf(layout string, objs ...interface{}) error {
	return fmt.Errorf("%v: %v", getName(1), fmt.Sprintf(layout, objs...))
}

func New(err error, objs ...interface{}) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("[%v%v]: %v", getName(1), getFormat(objs), err)
}

func Wrap(err error, msg string) error {
	return pkgErrors.Wrap(err, msg)
}

func Wrapf(err error, msg string, args ...interface{}) error {
	return pkgErrors.Wrapf(err, msg, args...)
}

func Casuse(err error) error {
	return pkgErrors.Cause(err)
}
