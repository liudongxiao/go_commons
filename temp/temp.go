package temp

import (
	"github.com/spf13/afero"
	"io/ioutil"
	"os"
)

func Open(name string) afero.File {
	fs := new(afero.MemMapFs)
	f, err := afero.TempFile(fs, "", name+"_")
	if err != nil {
		panic(err)
	}
	return f
}

func OpenWithData(name string, data []byte) afero.File {
	f := Open(name)
	if _, err := f.Write(data); err != nil {
		panic(err)
	}
	if _,err:=f.Seek(0,0);err!=nil{
		panic(err)
	}
	return f
}

func CetFiFoName(data []byte)(string,string)  {
	fin,err:=ioutil.TempFile("","fi")
	if err!=nil{
		panic(err)
	}
	if _,err:=fin.Write(data);err!=nil {
		panic(err)
	}
	fout,err:=ioutil.TempFile("","fo")
	if err!=nil{
		panic(err)
	}
	return fin.Name(),fout.Name()
}

func CetFiFo(data []byte)(*os.File,*os.File) {
	fin, err := ioutil.TempFile("", "fi")
	if err != nil {
		panic(err)
	}
	if _, err := fin.Write(data); err != nil {
		panic(err)
	}
	fout, err := ioutil.TempFile("", "fo")
	if err != nil {
		panic(err)
	}
	return fin, fout
}