package mongo

import (
	"dmp_web/go/commons/env"
	"dmp_web/go/commons/log"
	"sync"
)

var (
	globalCfgDsp     *Config
	globalCfg        *Config
	mongoOnce        sync.Once
	mongoInstance    *Mdb
	mongoInstanceDsp *Mdb
)

func NewMongoDB() *Mdb {
	mongoOnce.Do(func() {
		if globalCfg == nil {
			env.GetTestConf(&globalCfg)
		}
		mongoInstance = NewMdbWithConf(globalCfg)
	})
	return mongoInstance
}

func MdbDsp() *Mdb {
	if globalCfgDsp == nil {
		log.Error("mongo dsp config is null")
		return nil
	}
	if mongoInstanceDsp != nil {
		return mongoInstanceDsp

	}
	mongoInstanceDsp = NewMdbWithConf(globalCfgDsp)
	return mongoInstanceDsp
}

//func init(cfgs []*Config) {
//	if len(cfgs) == 0 {
//		panic("nil config")
//	} else if len(cfgs) == 1 && cfgs[0] == nil || cfgs[1] == nil {
//		panic("one of mongo configs is nil")
//	} else {
//		globalCfg = cfgs[0]
//		globalCfgDsp = cfgs[1]
//	}
//}

func Init(cfgs ...*Config) error {
	if len(cfgs) == 0 {
		panic("nil cfgs ")
	}
	for _, cfg := range cfgs {
		if cfg == nil {
			panic("cfg nil")
		}
	}
	if len(cfgs) == 1 {
		globalCfg = cfgs[0]
	} else if len(cfgs) >= 2 {
		globalCfg = cfgs[0]
		globalCfgDsp = cfgs[1]
	}
	return nil

}
