package reader

import (
	"bufio"
	"github.com/spf13/afero"
	"io"
	"strings"
)
//for test only
func TempFileFunc(name string, data []byte, f func(r io.ReadWriteCloser) error) (err error) {
	appFS := afero.NewMemMapFs()
	if name == "" {
		name = "test"
	}

	defer appFS.Remove(name)
	if err = afero.WriteFile(appFS, name, data, 0644); err != nil {
		return err
	}
	file, err := appFS.Open(name)
	if err != nil {
		return err
	}
	return f(file)
}


//for test only
func TempFile(name string) afero.File {
   f,err:=afero.TempFile( afero.NewMemMapFs(),"",name)
   if err!=nil{
   	panic(err)
   }
   return f
}

func TempFileWithData(name string,data []byte) afero.File {
	f:=TempFile(name)
	_,err:=f.Write(data)
	if err!=nil{
		panic(err)
	}
	return f
}

func TempFileWithDataFunc(name string,data []byte,cf func(line string) string) afero.File {
	f:=TempFile(name)
	bf:=bufio.NewScanner(strings.NewReader(string(data)))
	for bf.Scan(){
		line:=bf.Text()
		_,err:=f.Write([]byte(cf(line)))
		if err!=nil{
			panic(err)
		}
	}
	if err:=bf.Err();err!=nil{
		panic(err)
	}
	if err:=f.Sync();err!=nil{
		panic(err)
	}


	return f
}