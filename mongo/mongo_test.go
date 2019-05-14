package mongo

import (
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"
)

func GetMDb() *Mdb {
	return NewMdbWithConf(&Config{
		Host: "192.168.10.41",
		Port: 28001,
		DB:   "dmp",
	})
}

func TestMongoConn(t *testing.T) {
	db := GetMDb()
	if err := db.Session().Ping(); err != nil {
		t.Errorf("连接Mongo失败! %+v", err)
	}
	t.Log("mongo ping successfully!")
}

func TestSearch_1(t *testing.T) {
	db := GetMDb()
	query := bson.M{
		// NOTICE: 日期检查仅用于同步更新开启的时候
		"DateBegin": bson.M{"$lte": time.Now().Unix()},
		"DateEnd":   bson.M{"$gte": time.Now().Unix()},
		"$or": []bson.M{
			{"Type": "group", "AuthIds": 166},
			{"Type": "tag", "AuthIds": 166},
		},
	}
	var result []interface{}
	db.All("Auth", query, &result)
	t.Logf("%+v", result)

	var res interface{}
	query = bson.M{
		"_id": 1,
	}
	if err := db.One("Rule", query, &res); err != nil {

	}
	t.Logf("%+v", res)
}
