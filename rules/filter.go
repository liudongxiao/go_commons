package rules

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

type Filter struct {
	tree  *Tree
	query *Query
}

func NewFilter() *Filter {
	return &Filter{
		tree: NewTree(),
	}
}

func (f *Filter) Do(c *Collection) {

}

func (f *Filter) ReadCSVFile(fp string) (*Collection, error) {
	fd, err := os.Open(fp)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	return f.ReadCSV(fd)
}

func (f *Filter) GetResult(col *Collection) *TmpTable {
	tbl := NewTmpTable()
	group := f.query.Group

	if group != nil {
		keys := group.Columns()
		col.Sort(keys)
		for _, item := range group.List {
			item.SetColumnIndex(len(tbl.Schema))
			tbl.Schema = append(tbl.Schema, item)
		}
		for _, item := range f.query.GetHavingFuncs() {
			item.SetColumnIndex(len(tbl.Schema))
			tbl.Schema = append(tbl.Schema, item)
		}
		col.Reset()
		for col.Next() {
			tbl.Append(col, len(keys))
		}
	} else {
		for _, item := range f.query.Select.List {
			item.SetColumnIndex(len(tbl.Schema))
			tbl.Schema = append(tbl.Schema, item)
		}
		col.Reset()
		for col.Next() {
			tbl.Append(col, 0)
		}
	}
	if having := f.query.Having; having != nil {
		tbl.HavingFilter(having)
	}
	return tbl
}

func (f *Filter) ReadCSV(r io.Reader) (*Collection, error) {
	csvReader := csv.NewReader(r)
	fields, err := csvReader.Read()
	if err != nil {
		return nil, err
	}

	queryFields := f.GetColumns()
	queryFieldIdx := make([]int, len(queryFields))
	for i, f := range queryFields {
		idx := stringsIdx(f, fields)
		if idx == -1 {
			return nil, fmt.Errorf("source missing field: %v, got: %v", f, fields)
		}
		queryFieldIdx[i] = idx
	}
	col := &Collection{
		Schema: Schema(queryFields),
		Rows:   make([]Row, 0, 1<<10),
	}
	col.Reset()

	var record []string
readLine:
	for {
		record, err = csvReader.Read()
		if err != nil {
			break
		}
		row := make(Row, len(queryFields))
		for i, idx := range queryFieldIdx {
			if idx >= len(record) {
				continue readLine
			}
			row[i] = record[idx]
		}
		col.Offset++
		col.Rows = append(col.Rows[:col.Offset], row)
		if f.query.Where != nil {
			if !f.query.Where.Filter(col) {
				col.Rows = col.Rows[:col.Offset]
				col.Offset--
			}
		}
	}

	if err == io.EOF {
		err = nil
	}
	col.Reset()
	return col, nil
}

func (f *Filter) findFuncNode(n BinNode) *FuncNode {
	if fn, ok := n.Left().(*FuncNode); ok {
		return fn
	}
	if fn, ok := n.Right().(*FuncNode); ok {
		return fn
	}
	return nil
}

func (f *Filter) Process(statement string) (err error) {
	defer func() {
		panicErr := recover()
		if panicErr != nil {
			err = fmt.Errorf("%v", panicErr)
		}
	}()
	f.tree.Parse(statement)
	query, err := newParse().Parse(f.tree)
	if err != nil {
		return
	}
	f.query = query

	if f.query.Select == nil || len(f.query.Select.List) == 0 {
		return fmt.Errorf("invalid rule: missing projection: '%v'", statement)
	}
	if f.query.Where == nil {
		return fmt.Errorf("invalud rule: missing where: '%v'", statement)
	}
	if f.query.Where == nil {
		return fmt.Errorf("invalud rule: missing where")
	}
	return
}

func (f *Filter) GetColumns() []string {
	var column []string
	for _, node := range f.tree.Root.Nodes {
		for _, col := range node.Columns() {
			if stringsIdx(col, column) == -1 {
				column = append(column, col)
			}
		}
	}
	return column
}
