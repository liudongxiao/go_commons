package reader

import (
	"bufio"
	"bytes"
	"io"
)

//split big file
func Spilt(in io.Reader, maxLine int) []io.Reader {
	boArr := []*bytes.Buffer{}
	i := 0
	bo := &bytes.Buffer{}
	bf := bufio.NewScanner(in)
	for bf.Scan() {
		line := bf.Text()
		bo.Write([]byte(line + "\n"))
		i++
		if i%maxLine == 0 {
			boArr = append(boArr, bo)
			bo = &bytes.Buffer{}
			i = 0
		}
	}
	if i != 0 {
		boArr = append(boArr, bo)
	}
	ret := make([]io.Reader, len(boArr))
	for i, v := range boArr {
		ret[i] = v
	}
	return ret
}
