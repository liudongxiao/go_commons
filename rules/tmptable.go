package rules

type TblRow []Value
type TblSchema struct {
	Node ColumnNode
}

type TmpTable struct {
	Schema []ColumnNode
	Rows   []TblRow

	Offset int
}

func NewTmpTable() *TmpTable {
	return &TmpTable{
		Offset: -1,
	}
}

func (t *TmpTable) Reset() {
	t.Offset = -1
}

func (t *TmpTable) GetValueByIdx(idx int) Value {
	return t.Rows[t.Offset][idx]
}

func (t *TmpTable) GetValue(name string) Value {
	panic("not supported")
}

func (t *TmpTable) HavingFilter(filter FilterNode) {
	offset := 0
	t.Reset()
	for t.Next() {
		if filter.Filter(t) {
			t.Rows[offset] = t.GetRow()
			offset++
		}
	}
	t.Rows = t.Rows[:offset]
}

func (t *TmpTable) Append(c *Collection, n int) {
	startNewLine := false
	if t.Offset < 0 || n == 0 {
		startNewLine = true
	} else {
		for i := 0; i < n; i++ {
			val, _ := t.Schema[i].GetAggregateValue(c, i, t)
			if !val.Equal(t.Rows[t.Offset][i]) {
				startNewLine = true
				break
			}
		}
	}
	startCol := n
	if startNewLine {
		startCol = 0
		t.Offset++
		t.Rows = append(t.Rows, make(TblRow, len(t.Schema)))
	}

	for idx := startCol; idx < len(t.Schema); idx++ {
		col := t.Schema[idx]
		val, err := col.GetAggregateValue(c, idx, t)
		if err != nil {
			continue
		}
		t.Rows[t.Offset][idx] = val
	}
}

func (t *TmpTable) Next() bool {
	if t.Offset+1 >= len(t.Rows) {
		return false
	}
	t.Offset++
	return true
}

func (t *TmpTable) GetRow() TblRow {
	return t.Rows[t.Offset]
}
