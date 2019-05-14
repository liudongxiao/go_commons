package rules

import "sort"

type Column struct {
	Name string
}

type Schema []string

func (s Schema) Find(name string) int {
	return stringsIdx(name, s)
}

type Row []string

type sortCollection struct {
	col    *Collection
	fields []int
}

func newSortCollection(col *Collection, fields []string) *sortCollection {
	fs := make([]int, len(fields))
	for idx, field := range fields {
		fs[idx] = col.GetIdx(field)
	}
	return &sortCollection{
		col:    col,
		fields: fs,
	}
}

func (c *sortCollection) Len() int {
	return len(c.col.Rows)
}

func (c *sortCollection) Less(i, j int) bool {
	rowi := c.col.Rows[i]
	rowj := c.col.Rows[j]
	for _, idx := range c.fields {
		cmp := NewValue(rowi[idx]).Cmp(NewValue(rowj[idx]))
		if cmp == 0 {
			continue
		}
		return cmp < 0
	}
	return false
}

func (c *sortCollection) Swap(i, j int) {
	c.col.Rows[i], c.col.Rows[j] = c.col.Rows[j], c.col.Rows[i]
}

// -----------------------------------------------------------------------------

type Collection struct {
	Schema Schema
	Rows   []Row
	Offset int
}

func NewCollection() *Collection {
	return &Collection{
		Offset: -1,
	}
}

func (c *Collection) Sort(fields []string) {
	sort.Sort(newSortCollection(c, fields))
}

func (c *Collection) GetIdx(field string) int {
	return c.Schema.Find(field)
}

func (c *Collection) SetSchema(fields []string) {
	c.Schema = Schema(fields)
}

func (c *Collection) Reset() {
	c.Offset = -1
}

func (c *Collection) GetRow() Row {
	return c.Rows[c.Offset]
}

func (c *Collection) Next() bool {
	if c.Offset+1 >= len(c.Rows) {
		return false
	}
	c.Offset++
	return true
}

func (c *Collection) GetValue(name string) Value {
	idx := c.Schema.Find(name)
	return NewValue(c.Rows[c.Offset][idx])
}

func (c *Collection) GetValueByIdx(idx int) Value {
	return NewValue(c.Rows[c.Offset][idx])
}
