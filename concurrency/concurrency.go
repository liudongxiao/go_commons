
package concurrency

import (
	"bufio"
	"dmp_web/go/commons/log"
	"encoding/json"
	"io"
	"os"
	"runtime"
	"sync"
)



func readLine(fin io.Reader, hookfn func(interface{})) {
	scanner := bufio.NewScanner(fin)
	for scanner.Scan() {
		hookfn(scanner.Text())
	}
	close(inC)

	return
}

func readLineRet(file string, hookfn func(interface{}) interface{}) {
	fin, err := os.Open(file)
	if err != nil {
		log.Errorf("Fail to open file", err)
		return
	}
	defer fin.Close()

	scanner := bufio.NewScanner(fin)
	for scanner.Scan() {
		outC <- hookfn(scanner.Text())
	}
	close(inC)
	close(outC)

	return
}

func process(num int, f func(line <-chan interface{})) {
	for i := 0; i < num; i++ {
		wg.Add(1)
		go wrap(f)(inC)
	}
	wg.Wait()
	close(outC)
}

var inC = make(chan interface{}, runtime.NumCPU())
var outC = make(chan interface{}, runtime.NumCPU())
var wg sync.WaitGroup

func read(file io.Reader) error {
	cnt := 0
	readLine(file, func(line interface{}) {
		inC <- line
		cnt += 1
	})

	return nil

}

func write(file io.Writer) error {
	bf := bufio.NewWriter(file)
	for w := range outC {
		v, ok := w.(string);
		if ok {
			bf.Write([]byte(v))
		}else {
			v,err:=json.Marshal(w)
			if err!=nil{
				panic(err)
			}
			bf.Write(v)
		}
	}
	if err := bf.Flush(); err != nil {
		return err
	}

	return nil
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
	if err := read(fi); err != nil {
		return err
	}
	process(num, wrapChan(f))
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
	if err := read(fi); err != nil {
		return err
	}
	process(num, wrapChanRet(f))

	return write(fo)

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
