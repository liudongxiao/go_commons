package access

import (
	"dmp_web/go/commons/access/models"
	"dmp_web/go/commons/access/util"
	"dmp_web/go/commons/db/mongo"

	"gopkg.in/mgo.v2/bson"
)

type RbacMdbEngine struct {
	mdb *mongo.Mdb
}

func NewMdbEngine(mdb *mongo.Mdb) *RbacEngine {
	var engine = &RbacMdbEngine{
		mdb: mdb,
	}
	var res = &RbacEngine{}
	res.Rbac = engine
	return res
}

//判断是否有 paths 授权
func (rbac *RbacMdbEngine) CheckAll(userId int64, paths ...string) bool {

	var privPaths = rbac.GetPrivPaths(userId)
	if len(paths) > len(privPaths) {
		return false
	}
	return util.Contains(privPaths, paths...)
}

func (rbac *RbacMdbEngine) CheckAny(userId int64, paths ...string) bool {

	var privPaths = rbac.GetPrivPaths(userId)
	if len(paths) > len(privPaths) {
		return false
	}
	return util.ContainsAny(privPaths, paths...)
}

//判断是否有 functions ids的 授权  any , all
func (rbac *RbacMdbEngine) JudgeAny(userId int64, funcIds ...int64) bool {
	var functions = rbac.GetFunctions(userId)
	return util.ContainsAnyFunction(functions, funcIds...)
}

func (rbac *RbacMdbEngine) JudgeAll(userId int64, funcIds ...int64) bool {
	var functions = rbac.GetFunctions(userId)
	return util.ContainsFunctions(functions, funcIds...)
}

//获取用户的授权列表
func (rbac *RbacMdbEngine) GetFunctions(userId int64) []*models.Functions {
	var functions = []*models.Functions{}
	var functionIds = rbac.GetFunctionIds(userId)
	rbac.mdb.All(Functions, bson.M{"_id": bson.M{"$in": functionIds}, "IsDeleted": false}, &functions)
	return functions
}

//获取用户的functionIds
func (rbac *RbacMdbEngine) GetFunctionIds(userId int64) []int64 {
	userPriv := rbac.GetUserPriv(userId)
	var functionIds = make([]int64, 0, 10)
	if userPriv == nil {
		return functionIds
	}
	roles := []models.Role{}
	err := rbac.mdb.All(Role, bson.M{"_id": bson.M{"$in": userPriv.Roles}, "IsDeleted": false}, &roles)
	if err != nil {
		return functionIds
	}

	for _, role := range roles {
		functionIds = append(functionIds, role.Privileges...)
	}
	functionIds = append(functionIds, userPriv.Permissions...)
	functionIds = util.UnqiInt64Slice(functionIds)
	return functionIds
}

//获取 UserPrivilege
func (rbac *RbacMdbEngine) GetUserPriv(userId int64) *models.UserPrivilege {
	var result = &models.UserPrivilege{}
	err := rbac.mdb.One(UserPrivilege, bson.M{"UserId": userId, "IsDeleted": false}, result)
	if err != nil {
		return nil
	}
	return result
}

func (rbac *RbacMdbEngine) GetRoles(userid int64) []*models.Role {
	var roles = []*models.Role{}
	userPrivilege := rbac.GetUserPriv(userid)
	if userPrivilege == nil {
		return roles
	}
	rbac.mdb.All(Role, bson.M{"_id": bson.M{"$in": userPrivilege.Roles}, "IsDeleted": false}, &roles)
	return roles
}

func (rbac *RbacMdbEngine) GetPrivPaths(userId int64) []string {
	var functions = []*models.Functions{}
	var functionIds = rbac.GetFunctionIds(userId)
	rbac.mdb.All(Functions, bson.M{"_id": bson.M{"$in": functionIds}, "IsDeleted": false}, &functions)
	var res = make([]string, 0, len(functions))

	for _, function := range functions {
		res = append(res, function.Path)
	}
	return res
}
