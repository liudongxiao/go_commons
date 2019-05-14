package access

import (
	"dmp_web/go/commons/access/models"
)

const (
	UserPrivilege = "UserPrivilege"
	Privilege     = "Privilege"
	Role          = "Role"
	Functions     = "Functions"
)

//Rbac权限模型
//User ----> UserPrivilege -> Role---------------------
//                   |           |                     |
//                   |           Privilege --> Functions
//                   |-----Permissions ---------------|

type EngineType int

const (
	MONGO EngineType = 1
	MYSQL EngineType = 2
)

type Rbac interface {

	//core methods
	//判断是否有 paths 授权
	CheckAll(userId int64, paths ...string) bool
	CheckAny(userId int64, paths ...string) bool

	//判断是否有 ids 授权
	JudgeAny(userId int64, funcIds ...int64) bool
	JudgeAll(userId int64, funcIds ...int64) bool

	GetFunctions(userId int64) []*models.Functions
	GetFunctionIds(userId int64) []int64
	GetRoles(userid int64) []*models.Role
	GetUserPriv(userId int64) *models.UserPrivilege
	GetPrivPaths(userid int64) []string
}

type RbacEngine struct {
	Rbac
}
