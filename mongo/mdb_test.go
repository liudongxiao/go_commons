package mongo

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Boby struct {
	Id   int64  `bson:"_id"`
	Name string `bson:"Name"`
}

type Te struct {
	Name string `bson:"Name"`
}

func TestMdb(t *testing.T) {
	var colName = "a_collection_just_use_for_test"
	var err error
	db := NewMdbWithHost("localhost")

	Convey("测试mdb", t, func() {

		err = db.WithC(colName, func(c *mgo.Collection) error {
			_, e := c.RemoveAll(nil)
			return e
		})
		So(err, ShouldBeNil)

		err = db.Insert(colName, &Boby{Name: "Lulu", Id: 123})
		So(err, ShouldBeNil)
		err = db.Insert(colName, &Boby{Name: "Lulu1", Id: 124}, &Boby{Name: "Lulu2", Id: 125})
		So(err, ShouldBeNil)

		var result = &Boby{}
		db.One(colName, bson.M{"Name": "Lulu"}, result)
		So(result.Name, ShouldEqual, "Lulu")

		err = db.Update(colName, bson.M{"_id": 123}, bson.M{"Name": "SuperLulu"})
		So(err, ShouldBeNil)

		err = db.UpdateAll(colName, bson.M{"_id": 123}, bson.M{"$set": bson.M{"Name": "SuperLulu1"}})
		So(err, ShouldBeNil)

		So(db.Count(colName, bson.M{"Name": "SuperLulu1"}), ShouldEqual, 1)
		So(db.Exist(colName, bson.M{"Name": "SuperLulu1"}), ShouldBeTrue)
		So(db.ExistId(colName, 123), ShouldBeTrue)

		var arr []Boby
		err = db.All(colName, bson.M{"_id": 123}, &arr)
		So(err, ShouldBeNil)
		So(arr[0].Name, ShouldEqual, "SuperLulu1")

		err = db.RemoveAll(colName, bson.M{})
		So(err, ShouldBeNil)

		So(db.Count(colName, bson.M{"Name": "SuperLulu1"}), ShouldEqual, 0)
		So(db.Exist(colName, bson.M{}), ShouldBeFalse)

		err = db.All(colName, nil, &arr)
		So(err, ShouldBeNil)
		So(arr, ShouldBeEmpty)
	})
}

func TestMdb1(t *testing.T) {
	var colName = "a_collection_just_use_for_test"
	var err error
	db := NewMdbWithHost("localhost")

	Convey("测试mdb", t, func() {

		err = db.WithC(colName, func(c *mgo.Collection) error {
			_, e := c.RemoveAll(nil)
			return e
		})
		So(err, ShouldBeNil)

		err = db.Insert(colName, &Boby{Name: "Lulu", Id: 123})
		So(err, ShouldBeNil)
		err = db.Insert(colName, &Boby{Name: "Lulu1", Id: 124}, &Boby{Name: "Lulu2", Id: 125})
		So(err, ShouldBeNil)

		var result = &Te{}
		db.One(colName, bson.M{"Name": "Lulu"}, result)
		So(result.Name, ShouldEqual, "Lulu")
		t.Log(result)

		err = db.Update(colName, bson.M{"_id": 123}, bson.M{"Name": "SuperLulu"})
		So(err, ShouldBeNil)

		err = db.UpdateAll(colName, bson.M{"_id": 123}, bson.M{"$set": bson.M{"Name": "SuperLulu1"}})
		So(err, ShouldBeNil)

		So(db.Count(colName, bson.M{"Name": "SuperLulu1"}), ShouldEqual, 1)
		So(db.Exist(colName, bson.M{"Name": "SuperLulu1"}), ShouldBeTrue)
		So(db.ExistId(colName, 123), ShouldBeTrue)

		var arr []Boby
		err = db.All(colName, bson.M{"_id": 123}, &arr)
		So(err, ShouldBeNil)
		So(arr[0].Name, ShouldEqual, "SuperLulu1")

		err = db.RemoveAll(colName, bson.M{})
		So(err, ShouldBeNil)

		So(db.Count(colName, bson.M{"Name": "SuperLulu1"}), ShouldEqual, 0)
		So(db.Exist(colName, bson.M{}), ShouldBeFalse)

		err = db.All(colName, nil, &arr)
		So(err, ShouldBeNil)
		So(arr, ShouldBeEmpty)
	})
}

func TestMdb2(t *testing.T) {
	var colName = "a_collection_just_use_for_test"
	var err error
	db := NewMdbWithHost("localhost")

	Convey("测试mdb", t, func() {

		err = db.WithC(colName, func(c *mgo.Collection) error {
			_, e := c.RemoveAll(nil)
			return e
		})
		So(err, ShouldBeNil)
		var ret Boby
		err = db.UpsertNoId(colName, nil, &Boby{Name: "dong"}, &ret)
		So(err, ShouldBeNil)
		t.Logf("id %v", ret.Id)

	})
}
