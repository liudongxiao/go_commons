package reader

import (
	"bufio"
	"io"
)

func Map(f io.ReadWriteCloser, tf func(line string)string)(err error) {
	reader := bufio.NewScanner(f)
	writer:=bufio.NewWriter(f)
	for reader.Scan() {
		line := reader.Text()
		line = tf(line)
		writer.Write([]byte(line))
	}
	if err:=reader.Err();err!=nil{
		return err
	}
	if err:=writer.Flush();err!=nil{
		return err
	}

	return nil
}