package reader

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path"
	"strings"
	"time"
)

var ErrEmpty = errors.New("empty file")

//产生单一路径, txt 文件格式
func UnixFilePath(inFiles []string) []string {
	outFiles := make([]string, 0, len(inFiles))
	for _, inFile := range inFiles {
		fix := time.Now().Format("20060102150405")
		ext := path.Ext(inFile)
		base := path.Base(inFile)
		dir := path.Dir(inFile)
		noExtBase := strings.TrimRight(base, ext)
		fixBase := noExtBase + fix + ".txt"
		outFile := path.Join(dir, fixBase)
		outFiles = append(outFiles, outFile)
	}
	return outFiles

}

func MultiFileLines(equipmentType int, files ...string) (total, notValid,
	valid int64, err error) {
	r, err := MultiFileReader(files...)
	if err != nil {
		return 0, 0, 0, err
	}
	defer r.Close()
	t, n, v, err := FileLines(equipmentType, r)
	if err != nil {
		return 0, 0, 0, err
	}

	return t, n, v, nil
}

func readOp(f func(line string) error, r io.Reader) {
	input := bufio.NewScanner(r)
	for input.Scan() {
		if err := f(input.Text()); err != nil {
			break
		}

	}
	return

}

type multiReadCloser struct {
	closers []io.Closer
	reader  io.Reader
}

func (mc *multiReadCloser) Close() error {
	var err error
	for i := range mc.closers {
		err = mc.closers[i].Close()
	}
	return err
}

func (mc *multiReadCloser) Read(p []byte) (int, error) {
	return mc.reader.Read(p)
}

func MultiReadCloser(readClosers ...io.ReadCloser) io.ReadCloser {
	if len(readClosers) == 0 {
		return nil
	}
	cs := make([]io.Closer, len(readClosers))
	rs := make([]io.Reader, len(readClosers))
	for i := range readClosers {
		if readClosers[i] == nil {
			continue
		}
		cs[i] = readClosers[i]
		rs[i] = readClosers[i]
	}
	r := io.MultiReader(rs...)
	return &multiReadCloser{cs, r}
}

func MultiFileOpen(files ...string) ([]io.ReadCloser, error) {
	rc := make([]io.ReadCloser, 0, len(files))
	if len(files) == 0 {
		return nil, ErrEmpty
	}
	for _, f := range files {
		if f == "" {
			continue
		}
		fd, err := os.Open(f)
		if err != nil {
			return nil, err
		}
		rc = append(rc, fd)
	}
	return rc, nil
}

func MultiFileReader(files ...string) (io.ReadCloser, error) {
	if len(files) == 0 {
		return nil, ErrEmpty
	}
	rc, err := MultiFileOpen(files...)
	if err != nil {
		return nil, err
	}
	return MultiReadCloser(rc...), nil
}

type multiWriteCloser struct {
	closers []io.Closer
	writer  io.Writer
}

func MultiWriteCloser(readClosers ...io.WriteCloser) io.WriteCloser {
	cs := make([]io.Closer, len(readClosers))
	ws := make([]io.Writer, len(readClosers))
	for i := range readClosers {
		if readClosers[i] == nil {
			continue
		}
		cs[i] = readClosers[i]
		ws[i] = readClosers[i]
	}
	w := io.MultiWriter(ws...)
	return &multiWriteCloser{cs, w}
}

func (mc *multiWriteCloser) Close() error {
	var err error
	for i := range mc.closers {
		err = mc.closers[i].Close()
	}
	return err
}
func (mc *multiWriteCloser) Write(p []byte) (int, error) {
	return mc.writer.Write(p)
}

func MultiFileWriter(file ...string) (io.WriteCloser, error) {
	rc, err := MultiFileCreate(file...)
	if err != nil {
		return nil, err
	}
	return MultiWriteCloser(rc...), nil
}

func MultiFileCreate(files ...string) ([]io.WriteCloser, error) {
	rc := make([]io.WriteCloser, 0, len(files))
	if len(files) == 0 {
		return nil, ErrEmpty
	}
	for _, f := range files {
		if f == "" {
			continue
		}

		fd, err := os.Create(f)
		if err != nil {
			return nil, err
		}
		rc = append(rc, fd)
	}
	return rc, nil
}
