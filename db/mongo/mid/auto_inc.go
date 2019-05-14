package mid

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	field = &Field{
		Id:         "seq",
		Collection: "_id",
	}
)

type Field struct {
	Id         string
	Collection string
}

// 如果不设置，则用默认设置
func SetFieldName(id, collection string) {
	field.Id = id
	field.Collection = collection
}

//使collection 为 name 的 id 自增 1 并返回当前 id 的值
func AutoInc(c *mgo.Collection, name string) (id int, err error) {
	result := make(map[string]interface{})
	change := mgo.Change{
		Update:    bson.M{"$inc": bson.M{field.Id: 1}},
		Upsert:    true,
		ReturnNew: true,
	}
	_, err = c.Find(bson.M{field.Collection: name}).Apply(change, result)
	if err != nil {
		return
	}
	id = result[field.Id].(int)
	return
}
