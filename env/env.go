package env

import (
	"gopkg.in/redis.v5"

	"dmp_web/go/commons/db/hive"

	"github.com/colinmarc/hdfs"
)

var (
	isTest bool
	Debug  = false
	Test   = false
)

var (
	HiveDatabasePath string

	HadoopBin string

	HadoopNameNOde string
)

var (
	HCli *hive.Client

	HdfsCli *hdfs.Client
)

var (
	RootDir string

	ConfigString string

	Concurrence          int = 3
	TaskProcessFrequency     = 10
)

var (
	Notify *redis.Client

	RedisALLKey = "_all"

	RedisDoneKey = "_done"

	CacheSqlsSet = "cacheSqls"
)

const (
	PC  = 1
	Mob = 2

	PCStr  = "pc"
	MobStr = "mob"

	DspMob    = 4
	DspMobStr = "mobile"

	PcTable     = "dmpstage.dmp_uploader_pc_table"
	MobileTable = "dmpstage.dmp_uploader_mobile_table"
)

const (
	HadoopDir      = "/tmp/"
	HadoopUserName = "HADOOP_USER_NAME=root"
	HiveDatabase   = "dmpstage"
)

const (
	// 最大文件大小100m
	FileMaxsize = int64(52428800) * 2
	// 上传的hive表
)

//const TagID = "tagID"

const (
	Android_id_len  = 16
	Idfa_len        = 36
	Imei_len        = 15
	Mandroid_id_len = 32
	Sandroid_id_len = 40
	Midfa_len       = 32
	Sidfa_len       = 40
	Mimei_len       = 32
	Simei_len       = 40
)

var did_len = []int{Android_id_len, Idfa_len}

const (
	Android_id  = "android-id"
	Idfa        = "idfa"
	Imei        = "imei"
	Mandroid_id = "mandroid-id"
	Sandroid_id = "sandroid-id"
	Midfa       = "midfa"
	Sidfa       = "sidfa"
	Mimei       = "mimei"
	Simei       = "simei"
	Zero        = ""
	Did         = "did"
)

var TypeMap = map[string]int{
	Android_id:  16,
	Idfa:        36,
	Imei:        15,
	Mandroid_id: 32,
	Sandroid_id: 40,
	Midfa:       32,
	Sidfa:       40,
	Mimei:       32,
	Simei:       40,
	Zero:        0,
}

var (
	ExportHost string
	Token      string
	SupplierId string
)
var ( // cookie 长度是１５　或１６
	CookieLens = []int{15, 16}
	Timeout    = 24 * 7
)

const (
	DayFormat    = "20060102"
	SecondFormat = "20060102101010"
)
