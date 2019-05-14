package models

type UserPrivilege struct {
	Id           int64   `bson:"_id"`
	UserId       int64   `bson:"UserId"`
	Permissions  []int64 `bson:"Permissions"`
	Roles        []int64 `bson:"Roles"`
	CampanyId    int32   `bson:"CampanyId"`
	CreateUserId int64   `bson:"CreateUserId"`
	CreateTime   int64   `bson:"CreateTime"`
	IsDeleted    bool    `bson:"IsDeleted"`
	DeleteUserId int64   `bson:"DeleteUserId"`
	DeleteTime   int64   `bson:"DeleteTime"`
	UpdateUserId int64   `bson:"UpdateUserId"`
	UpdateTime   int64   `bson:"UpdateTime"`
}

type Privilege struct {
	Id          int64   `bson:"_id"`
	Name        string  `bson:"Name"`        //名称
	Code        string  `bson:"Code"`        //编码
	Value       []int64 `bson:"Value"`       //拥有的功能Id
	Description string  `bson:"Description"` //描述
	Type        int     `bson:"Type"`        //1功能，2指标
	ParentId    int64   `bson:"ParentId"`    //父权限Id，0表示根权限
	SortId      int     `bson:"SortId"`      ///排序Id，暂时未用到
}

type Role struct {
	Id           int64   `bson:"_id"`
	Name         string  `bson:"Name"`
	Note         string  `bson:"Note"`
	Privileges   []int64 `bson:"Privileges"`
	IsSystem     bool    `bson:"IsSystem"`
	CompanyId    int64   `bson:"CompanyId"`
	CreateUserId int64   `bson:"CreateUserId"`
	CreateTime   int64   `bson:"CreateTime"`
	IsDeleted    bool    `bson:"IsDeleted"`
	DeleteUserId int64   `bson:"DeleteUserId"`
	DeleteTime   int64   `bson:"DeleteTime"`
	UpdateUserId int64   `bson:"UpdateUserId"`
	UpdateTime   int64   `bson:"UpdateTime"`
}

type Functions struct {
	Id     int64  `bson:"_id"`
	Name   string `bson:"Name"`   //名称
	Info   string `bson:"Info"`   //信息
	Path   string `bson:"Path"`   //路径（Router）
	Status int    `bson:"Status"` //状态，0无效，1有效

	CreateUserId int64 `bson:"CreateUserId"`
	CreateTime   int64 `bson:"CreateTime"`
	IsDeleted    bool  `bson:"IsDeleted"`
	DeleteUserId int64 `bson:"DeleteUserId"`
	DeleteTime   int64 `bson:"DeleteTime"`
	UpdateUserId int64 `bson:"UpdateUserId"`
	UpdateTime   int64 `bson:"UpdateTime"`
}
