package access

import (
	_ "dmp_web/go/commons/access/models"
	"dmp_web/go/commons/db/mongo"
	_ "dmp_web/go/commons/util"
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var mdb = mongo.NewMdb("192.168.10.28", "27017", "Abtest", "", "")
var rbac = NewMdbEngine(mdb)

var userId = int64(3)

func TestCheck(t *testing.T) {
	Convey("TestCheck", t, func() {

		var paths = []string{"campany/listcustomer", "admin/listcharge"}
		flag := rbac.CheckAll(userId, paths...)
		So(flag, ShouldEqual, false)

		paths = []string{"/project/edit", "/access/controll"}
		flag = rbac.CheckAll(userId, paths...)
		So(flag, ShouldEqual, true)

	})
}

func TestGetFunctions(t *testing.T) {
	Convey("TestGetFunctions", t, func() {
		functions := rbac.GetFunctions(userId)
		So(len(functions), ShouldEqual, 3)

		ids := rbac.GetFunctionIds(userId)
		So(len(ids), ShouldEqual, 3)
	})
}

func TestGetUserPriv(t *testing.T) {

	var userPriv = rbac.GetUserPriv(userId)
	fmt.Printf("%#v\n", userPriv)
}

func TestGetPrivPaths(t *testing.T) {

	var paths = rbac.GetPrivPaths(userId)
	fmt.Printf("%#v\n", paths)
	Convey("TestGetPrivPaths", t, func() {
		So(len(paths), ShouldEqual, 3)
	})
}

func TestJudge(t *testing.T) {
	Convey("TestJudge", t, func() {

		flag := rbac.JudgeAll(userId, 2, 3, 4)
		So(flag, ShouldEqual, true)

		flag = rbac.JudgeAny(userId, 2, 5, 6)
		So(flag, ShouldEqual, true)
	})
}

func TestGetRoles(t *testing.T) {
	Convey("TestJudge", t, func() {

		roles := rbac.GetRoles(userId)
		So(len(roles), ShouldEqual, 1)
		So(roles[0].Id, ShouldEqual, 1)
	})
}
