package rules

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

var _ ValueGetter = &MapBuffer{}

type ColumnName []string

func NewColumnName(s string) ColumnName {
	return strings.Split(s, ".")
}

func (c ColumnName) Name() string {
	return strings.Join(c, ".")
}

func (c ColumnName) GetInMap(data map[string]interface{}) (interface{}, bool) {
	for i := 0; i < len(c)-1; i++ {
		obj, ok := data[c[i]]
		if !ok {
			return nil, ok
		}
		if obj, ok := obj.(map[string]interface{}); ok {
			data = obj
			continue
		}
		return nil, ok
	}
	ret, ok := data[c[len(c)-1]]
	return ret, ok

}

type MapBuffer struct {
	columns []ColumnName
	values  []Value

	projectionKey int
}

func (b *MapBuffer) GetValue(string) Value {
	panic("not support")
}
func (b *MapBuffer) GetValueByIdx(i int) Value {
	return b.values[i]
}

func (f *Filter) MapJSON(buffer *MapBuffer, data []byte) (Value, bool, error) {
	var ret map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&ret); err != nil {
		return NIL, false, nil
	}

	if buffer.columns == nil {
		indexMap := make(map[string]int)
		cols := f.GetColumns()
		buffer.columns = make([]ColumnName, len(cols))
		for idx, col := range cols {
			buffer.columns[idx] = NewColumnName(col)
			indexMap[col] = idx
		}
		buffer.values = make([]Value, len(buffer.columns))
		ins := f.query.GetWhereIdents()
		for _, in := range ins {
			idx, ok := indexMap[in.Name]
			if !ok {
				panic("internal error: column not found: " + in.Name)
			}
			in.SetColumnIndex(idx)
		}
		buffer.projectionKey = len(buffer.columns) - 1
	}

	for idx, col := range buffer.columns {
		val, ok := col.GetInMap(ret)
		if !ok {
			return NIL, false, fmt.Errorf("column not found: %v", col.Name())
		}
		buffer.values[idx] = NewValue(val)
	}

	if !f.query.Where.Filter(buffer) {
		return NIL, false, nil
	}

	return buffer.values[buffer.projectionKey], true, nil
}
