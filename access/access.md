## rbac权限系统的封装
mongodb 实现


## 使用 ##
引入相关包

```
import (
    "dmp_web/go/commons/access"
    "dmp_web/go/commons/access/models"
    "dmp_web/go/commons/db/mongo"
)
```

初始化

```
   var mdb = mongo.NewMdb("192.168.10.28", "27017", "Abtest", "", "")
   var rbac = NewMdbEngine(mdb)
```

判断权限

-判断所有path都满足

```
      var paths = []string{"campany/listcustomer", "admin/listcharge"}
      flag := rbac.CheckAll(userId, paths...)
```

-判断任一path

```
      flag := rbac.CheckAny(userId, paths...)
```

-判断是否有 functions ids的 授权

```
    rbac.JudgeAll(userId, 2, 3, 4)
    rbac.JudgeAny(userId, 2, 5, 6)
```

-获取用户的授权paths

```
func (rbac *Rbac) GetPrivPaths(userId int64) ([]string)
```

....

> models有权限各个model的声明，一些CURD的操作可以直接操作mongo实现， 这里仅仅基于mongo封装了一些通用检查权限的方法
 
