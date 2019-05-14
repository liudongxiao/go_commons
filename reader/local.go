package reader

import (
	"io"
	"os"
)

type Opener interface {
	Open(file string) (io.ReadCloser, error)
}

type localOpen func(string) (*os.File, error)
type LocalClient struct {
	lc localOpen
}

func NewLocalClient(fn localOpen) *LocalClient {
	var l LocalClient
	l.lc = fn
	return &l
}

func (l *LocalClient) Open(file string) (io.ReadCloser, error) {
	return l.lc(file)
}

type MultiOpener interface {
	MultiOpen(file ...string) io.ReadCloser
}

func (l *LocalClient) MultiOpen(files ...string) ([]io.ReadCloser, error) {
	return MultiFileOpen(files...)
}

type OpenDir interface {
	OpenDir(dir string, rec bool) (io.ReadCloser, error)
}

func (l *LocalClient) OpenDir(dir string, rec bool) (io.ReadCloser, error) {
	if rec {
		return RecurDirReader(dir)
	}
	return DirReader(dir)
}
