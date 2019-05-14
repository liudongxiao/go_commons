package errors

import (
	"io"
	"testing"
)

func ttt() error {
	return New(io.EOF, "hello")
}

func TestNew(t *testing.T) {
	println(Newf("hello! %v", 1).Error())
}

func TestWrap(t *testing.T) {
	println(ttt().Error())
}
