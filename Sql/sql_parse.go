package Sql

import "vitess.io/vitess/go/vt/sqlparser"

//TODO: test this func
func SqlTableNames(sql string) ([]string, error) {

	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, ErrUnvalid

	}

	var tables []string

	getTable := func(node sqlparser.SQLNode) (bool, error) {
		if node, ok := node.(sqlparser.SimpleTableExpr); ok {
			table := sqlparser.GetTableName(node).String()
			if table == "" {
				return false, ErrEmpty
			}
			tables = append(tables, table)
		}
		return false, nil
	}

	// Otherwise do something with stmt
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		sqlparser.Walk(getTable, stmt)
	case *sqlparser.Insert:
		sqlparser.Walk(getTable, stmt)
	case *sqlparser.Delete:
		sqlparser.Walk(getTable, stmt)
	}
	return tables, nil
}

func IsRightSql(sql string) error {
	_, err := sqlparser.Parse(sql)
	return err
}
