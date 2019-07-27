package concurrency

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"runtime"
	"sync"
)

func readLine(fin io.Reader, hookfn func(interface{})) error {
	scanner := bufio.NewScanner(fin)
	for scanner.Scan() {
		hookfn(scanner.Text())
	}
	return scanner.Err()

}

//func readLineRet(file string, hookfn func(interface{}) interface{}) {
//	fin, err := os.Open(file)
//	if err != nil {
//		log.Errorf("Fail to open file", err)
//		return
//	}
//	defer fin.Close()
//
//	scanner := bufio.NewScanner(fin)
//	for scanner.Scan() {
//		outC <- hookfn(scanner.Text())
//	}
//	close(inC)
//	close(outC)
//
//	return
//}

func process(num int, f func(line <-chan interface{})) error {
	for i := 0; i < num; i++ {
		wg.Add(1)
		go wrap(f)(inC)
	}
	wg.Wait()
	close(outC)
	return nil
}

var inC = make(chan interface{}, runtime.NumCPU())
var outC = make(chan interface{}, runtime.NumCPU())
var wg = new(sync.WaitGroup)
var cnt int64

func read(file io.Reader) error {
	err := readLine(file, func(line interface{}) {
		inC <- line
		cnt += 1
	})
	close(inC)
	return err
}

func write(file io.Writer) error {
	bf := bufio.NewWriter(file)
	for w := range outC {
		v, ok := w.(string)
		if ok {
			bf.Write([]byte(v))
		} else {
			v, err := json.Marshal(w)
			if err != nil {
				panic(err)
			}
			bf.Write(v)
		}
	}
	return bf.Flush()

}

func wrapChan(f func(line interface{})) func(line <-chan interface{}) {
	return func(line <-chan interface{}) {

		for line := range inC {
			f(line)
		}
	}
}

func wrapChanRet(f func(line interface{}) interface{}) func(line <-chan interface{}) {
	return func(line <-chan interface{}) {
		for line := range inC {
			outC <- f(line)
		}
	}
}

func Run(fi io.Reader, num int, f func(line interface{})) error {
	errC := make(chan error, 1)
	go func() {
		errC <- read(fi)
	}()
	go func() {
		errC <- process(num, wrapChan(f))
	}()
	for i := 1; i <= 2; i++ {
		if err := <-errC; err != nil {
			return err
		}
	}
	return nil
}

func RunFile(fin string, num int, f func(line interface{})) error {
	fi, err := os.Open(fin)
	if err != nil {
		return err
	}
	return Run(fi, num, f)

}

//RunFileRet use  gorutines to parallel to process f, f has interface{} return
func RunRet(fi io.Reader, fo io.Writer, num int, f func(line interface{}) interface{}) error {
	errC := make(chan error, 1)
	go func() {
		errC <- read(fi)
	}()

	go func() {
		errC <- process(num, wrapChanRet(f))
	}()
	go func() {
		errC <- write(fo)
	}()

	for i := 1; i <= 3; i++ {
		if err := <-errC; err != nil {
			return err
		}
	}
	return nil

}

//RunFileRet use  gorutines to parallel to process f, f has interface{} return , fin is input file,
// fout is output file ,  num is gorutines number
func RunFileRet(fin, fout string, num int, f func(line interface{}) interface{}) error {
	fi, err := os.Open(fin)
	if err != nil {
		return err
	}
	defer fi.Close()
	fo, err := os.Create(fout)
	if err != nil {
		return err
	}
	defer fo.Close()
	return RunRet(fi, fo, num, f)

}

func wrapRet(f func(line <-chan interface{}) interface{}) func(line <-chan interface{}) {
	return func(line <-chan interface{}) {
		outC <- f(line)
		wg.Done()
	}

}

func wrap(f func(line <-chan interface{})) func(line <-chan interface{}) {
	return func(line <-chan interface{}) {
		f(line)
		wg.Done()
	}

}
