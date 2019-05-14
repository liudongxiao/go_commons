package loginit

import "dmp_web/go/commons/log"

// 提供一个能在最开始就把 log 关闭的包
// 使用，在 main 里把包引入放在 import 的第一行,确保该包为最先引入的包
func init() {
	log.SetRootLevel(log.NO)
}
