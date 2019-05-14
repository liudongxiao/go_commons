package env

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"
	"reflect"
	"runtime"

	"dmp_web/go/commons/log"
)

func ReadConf(configFilePath string, obj interface{}) error {
	data, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		return err
	}

	// 把配置文件的 json 转成字符串放在 env.ConfigString 里面
	ConfigString = bytes.NewBuffer(data).String()
	if err != nil {
		log.Debug("new hive client error")

	}

	return json.Unmarshal(data, obj)

}

func GetConfByName(name string, obj interface{}) error {
	fp := filepath.Join(GetConfPath(), name)
	data, err := ioutil.ReadFile(fp)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, obj); err != nil {
		return err
	}
	return nil
}

func GetTestConf(objs ...interface{}) error {
	for _, obj := range objs {
		val := reflect.ValueOf(obj)
		if val.Kind() != reflect.Ptr {
			return fmt.Errorf("obj %v is not ptr type", obj)
		}
		valElem := val.Elem()
		typ := valElem.Type()
		name := typ.PkgPath()
		if name == "" && typ.Kind() == reflect.Ptr {
			name = typ.Elem().PkgPath()
		}
		name = path.Base(name)
		if err := GetConfByName(name+"_test.json", obj); err != nil {
			return err
		}
	}
	return nil
}

func GetConfPath() string {
	return filepath.Join(GetProjectRoot(), "conf")
}

func GetProjectRoot() string {
	_, fp, _, _ := runtime.Caller(0)
	ret, err := filepath.Abs(filepath.Dir(fp) + "../../../../")
	if err != nil {
		panic("can't find project directory")
	}

	return ret
}

func GetRootPath() string {
	if RootDir == "" {
		RootDir = GetProjectRoot()
	}

	return RootDir
}
