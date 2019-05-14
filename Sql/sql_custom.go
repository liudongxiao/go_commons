package Sql

import (
	"bytes"
	"errors"
	"strings"
)

func FormalCreateSql(sql, tableName string) (createTableSql, table string) {
	if sql == "" {
		return "", ""
	}
	//　去掉多余空格
	sql = strings.Join(strings.Fields(sql), " ")
	sqlUpper := strings.ToUpper(sql)
	if strings.Contains(sqlUpper, "CREATE TABLE ") {
		sqlTable := GetTableName(sql)
		return sql, sqlTable
	}
	buf := bytes.NewBuffer(nil)
	buf.WriteString("CREATE TABLE IF NOT EXISTS ")
	buf.WriteString(tableName)
	buf.WriteString("  as  ")
	buf.WriteString(sql)
	sql = buf.String()
	sql = strings.Join(strings.Fields(sql), " ")
	return sql, tableName

}

func GetTableName(sql string) string {
	words := strings.Fields(sql)
	for i := len(words) - 1; i >= 0; i-- {
		wordUpper := strings.ToUpper(words[i])
		if wordUpper == "EXISTS" {
			i++
			return words[i]
		}
		if wordUpper == "TABLE" {
			i++
			return words[i]
		}

	}
	return ""
}

func CheckSafeSql(sql string) error {
	//　去掉多余空格
	sql = strings.Join(strings.Fields(sql), " ")
	sql = strings.ToUpper(sql)
	if strings.Contains(sql, "DROP TABLE") {
		return errors.New("contain drop table sql ")
	}
	return nil
}

var ErrEmpty = errors.New("empty table")
var ErrUnvalid = errors.New("not valid sql")

//func saveSqlTable(sql string) error {
//	tables,err:=SqlTableNames(sql)
//	if err!=nil{
//		return err
//	}
//	saveToMongo
//}
