package Sql

import (
	"context"
	"dmp_web/go/commons/db/hive"
	"dmp_web/go/commons/env"
	"dmp_web/go/commons/errors"
	"dmp_web/go/commons/log"
	"fmt"
)

func CheckTableSql(table string) (sql string) {
	return fmt.Sprintf("SELECT COUNT(1) FROM %s", table)

}

func CheckTableResultNumber(ctx context.Context, table string) (int64, error) {
	var count int64

	checkSQL := CheckTableSql(table)

	res, err := env.HCli.ExecuteSyncCtx(ctx, checkSQL)
	if err != nil {
		return 0, err
	}
	if res != nil {
		if res.Next() {
			res.Scan(&count)

			if count == 0 {
				return 0, errors.Wrap(errors.ErrTableZero{table}, "")
			} else {
				return count, nil
			}
		}
	}
	return 0, errors.Wrap(errors.ErrTableRetNil{table}, "")
}

func DropTableSql(tableName string) (sql string) {
	sql = fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	return
}

func DropTable(ctx context.Context, hcli hive.Cli, tableName string) error {
	dropTableStatement := DropTableSql(tableName)

	_, err := hcli.ExecuteSyncCtx(ctx, dropTableStatement)

	if err == nil {
		log.Debugf("Clean table: %s successfully.", tableName)
		return nil
	}
	log.Error("Error HQL: ", dropTableStatement)
	return err
}

func CreateJoinBaiduTableSqls(createTable, distinctTable, fromTable string, etype int) (sqls []string, err error) {
	joinTable, id, err := getBaiduJoinTable(etype)
	if err != nil {
		return nil, err
	}
	createBaiduJoinTablSql := fmt.Sprintf(
		`
       CREATE TABLE IF NOT EXISTS %[1]s stored AS ORC AS
       SELECT wt.*
       FROM %[2]s AS ds
       JOIN %[3]s AS wt ON (wt.%[4]s= ds.%[4]s
                  AND ds.%[4]s IS NOT NULL AND ds.%[4]s != "")
	`, createTable, fromTable, joinTable, id)

	// 去重的 baiduJoinTable
	distinctTableSql := fmt.Sprintf(`
       CREATE TABLE IF NOT EXISTS %[1]s stored AS ORC AS
       SELECT DISTINCT *
       FROM %[2]s`, distinctTable, createTable)

	sqls = append(sqls, createBaiduJoinTablSql, distinctTableSql)
	return
}

func getBaiduJoinTable(etype int) (table string, selectField string, err error) {
	if etype == env.PC {
		table = "dmp.baidu_user_wide_table"
		selectField = "visitor_id"
		return
	} else if etype == env.Mob {
		table = "dmp.baidu_mobile_user_wide_table"
		selectField = "did"
		return
	}
	return "", "", errors.Wrap(errors.ErrType{etype}, "")

}

func CreateJoinBaiduTable(ctx context.Context, hcli hive.Cli, createTable, distinctTable, fromTable string, etype int) error {
	sqls, err := CreateJoinBaiduTableSqls(createTable, distinctTable, fromTable, etype)
	if err != nil {
		return err
	}

	if _, err := hcli.ExecuteSyncCtx(ctx, sqls[0]); err != nil {
		return errors.Wrap(errors.ErrTableCreat{createTable, err}, "")
	}

	num, err := CheckTableResultNumber(ctx, createTable)
	if err != nil {
		return err
	}

	log.Debugf("%s get a %d result", createTable, num)

	if _, err := hcli.ExecuteSyncCtx(ctx, sqls[1]); err != nil {
		return errors.Wrap(errors.ErrTableCreat{distinctTable, err}, "")
	}
	return nil
}

type BaiduTempTables struct {
	etype                       int
	inTable                     string
	BaiduJoinTable              string
	DistinctbaiduJoinBaiduTable string
}
