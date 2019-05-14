package reader

import (
	"dmp_web/go/cmd/ali_lookalike/go/common/generate_data"
	"dmp_web/go/commons/concurrency"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func GetDirFiles(dir string) ([]string, error) {
	fileInfo, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(fileInfo))
	for _, f := range fileInfo {

		files = append(files, path.Join(dir, f.Name()))
	}
	return files, nil
}

func DirReader(dir string) (io.ReadCloser, error) {
	files, err := GetDirFiles(dir)
	if err != nil {
		return nil, err
	}
	return MultiFileReader(files...)
}

func RecurDirFiles(root string) ([]string, error) {
	paths := []string{}
	if err := filepath.Walk(root, func(path string, f os.FileInfo, err error) error {
		if f.Mode().IsRegular() {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return paths, nil
}

func RecurDirReader(root string) (io.ReadCloser, error) {
	files, err := RecurDirFiles(root)
	if err != nil {
		return nil, err
	}
	return MultiFileReader(files...)
}

func ReplacePathExt(file, new string) string {
	old := path.Ext(file)
	if old == "" {
		return file + new
	}
	return strings.Replace(file, old, new, 1)
}

func pipeReader() (io.ReadCloser, error) {
	r, _ := io.Pipe()
	return r, nil

}

func RecurProcess(root, out string, f func(line interface{}) interface{}) error {
	paths, err := RecurDirFiles(root)
	if err != nil {
		return err
	}
	outPaths := generate_data.UnixFilePath(paths, out)
	for i, in := range paths {
		if err := concurrency.RunFileRet(in, outPaths[i], 0, f); err != nil {
			return err
		}
	}
	return nil
}
func RecurProcessDir(inDir, outDir string, name string, f func(line interface{}) interface{}) error {
	ipaths, err := RecurDirFiles(inDir)
	if err != nil {
		return err
	}
	opaths := RecurCopy(inDir, outDir, name)
	for i, in := range ipaths {
		if err := concurrency.RunFileRet(in, opaths[i], 0, f); err != nil {
			return err
		}
	}
	return nil

}

func RecurCopy(inDir string, outDir string, name string) []string {
	paths, err := RecurDirFiles(inDir)
	if err != nil {
		panic(err)
	}
	return generate_data.CopyFile(paths, outDir, name)

}
