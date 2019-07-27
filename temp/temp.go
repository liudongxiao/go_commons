package temp

import (
	"io/ioutil"
	"os"
)



func OpenWithData(name string, data []byte) *os.File {
	f,err := os.Open(name)
	panic(err)
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
	if _,err:=fin.Seek(0,0);err!=nil{
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
	if _,err:=fin.Seek(0,0);err!=nil{
		panic(err)
	}
	fout, err := ioutil.TempFile("", "fo")
	if err != nil {
		panic(err)
	}
	return fin, fout
}