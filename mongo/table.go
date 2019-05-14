package mongo

import (
	"reflect"
	"strings"

	"dmp_web/go/commons/db/mongo/mid"

	"dmp_web/go/commons/errors"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Session struct {
	*mgo.Collection
	sess *mgo.Session
}

func (s *Session) Close() {
	s.sess.Close()
}

type Table struct {
	name  string
	value reflect.Value
}

func NewTable(obj interface{}) *Table {
	value := reflect.ValueOf(obj)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	name := value.Type().Name()
	idField, ok := value.Type().FieldByName("Id")
	if !ok {
		panic("can't find field: id")
	}
	if !strings.Contains(idField.Tag.Get("bson"), "omitempty") {
		println(idField.Tag.Get("bson"))
		panic(errors.Newf("model %v: Id is missing omitempty", name))
	}

	t := &Table{
		value: value,
		name:  name,
	}
	return t
}

func (t *Table) Session() *Session {
	mdb := NewMongoDB()
	session := mdb.Session()
	return &Session{
		mdb.DB(session).C(t.name),
		session,
	}
}

// func (t *Table) Find(selector interface{}) error {
// 	return NewMongoDB().
// }

func (t *Table) FindId(id, result interface{}) error {
	return NewMongoDB().FindId(t.name, id, result)
}

func (t *Table) AllBySort(query, result interface{}, sorted ...string) error {
	return NewMongoDB().WithC(t.name, func(c *mgo.Collection) error {
		return c.Find(query).Sort(sorted...).All(result)
	})
}

func (t *Table) RemoveAll(query interface{}) error {
	return NewMongoDB().RemoveAll(t.name, query)
}

func (t *Table) Count(query interface{}) (int, error) {
	session := NewMongoDB().Session()
	n, err := NewMongoDB().DB(session).C(t.name).Find(query).Count()
	session.Close()
	return n, err
}

func (t *Table) One(query, result interface{}) error {
	return NewMongoDB().One(t.name, query, result)
}

func (t *Table) All(query, result interface{}) error {
	return NewMongoDB().All(t.name, query, result)
}

func (t *Table) Insert(data []interface{}) error {
	return NewMongoDB().WithC(t.name, func(c *mgo.Collection) error {
		return c.Insert(data...)
	})

}

func (t *Table) SaveId(id *int64, obj interface{}) error {
	tmp := *id
	*id = 0
	_, err := t.UpsertSet(bson.M{"_id": tmp}, obj)
	*id = tmp
	return err
}

// save anything without id
func (t *Table) Save(id *bson.ObjectId, obj interface{}) error {
	if *id == "" {
		*id = bson.NewObjectId()
	}
	tmp := *id
	*id = ""
	_, err := t.UpsertSet(bson.M{"_id": tmp}, obj)
	*id = tmp
	return err
}

func (t *Table) UpsertSet(selector, change interface{}) (info *mgo.ChangeInfo, err error) {
	err = NewMongoDB().WithC(t.name, func(c *mgo.Collection) error {
		info, err = c.Upsert(selector, bson.M{"$set": change})
		return err
	})
	return
}

func (t *Table) Exist(selector interface{}) bool {
	return NewMongoDB().Exist(t.name, selector)
}

func (t *Table) UpdateIdSet(id, change interface{}) error {
	return NewMongoDB().UpdateId(t.name, id, bson.M{"$set": change})
}

func (t *Table) Update(selector, change interface{}) error {
	return NewMongoDB().Update(t.name, selector, change)
}

// 底层upsert 有bug, info , err2 := c.Upsert(nil , upsertdata ) , info Id 为nil
func (t *Table) Upsert(selector, change interface{}) error {
	return NewMongoDB().Upsert(t.name, selector, change)
}

func (t *Table) UpdateSet(selector, change interface{}) error {
	return NewMongoDB().Update(t.name, selector, bson.M{"$set": change})
}

func (t *Table) UpdateSetAll(selector, change interface{}) error {
	return NewMongoDB().UpdateAll(t.name, selector, bson.M{"$set": change})
}

func (t *Table) FindByIds(ids, result interface{}) error {
	return t.All(bson.M{"_id": bson.M{"$in": ids}}, result)
}

func (t *Table) GetTableName() string {
	return t.name
}

//使用自增id来保存新纪录，自增ID通过inner.IdCounter表来维护，字段统一命名为tableName+Id
func (t *Table) SaveWithAutoIncId(obj interface{}) (int64, error) {
	if id, err := mid.AutoInc(t.Session().Database.C("inner.IdCounter"), t.name+"Id"); err != nil {
		return 0, errors.Wrap(err, "获取自增Id失败! collection:[inner.IdCounter], columnName:["+t.name+"Id]")
	} else {
		return int64(id), t.Upsert(bson.M{"_id": int32(id)}, obj)
	}
}
