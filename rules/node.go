package rules

import (
	"bytes"
	"fmt"
	"strconv"
)

type ValueGetter interface {
	GetValue(name string) Value
	GetValueByIdx(idx int) Value
}

type ValueNode interface {
	Node
	GetValue(c ValueGetter) Value
}

type FilterNode interface {
	BinNode
	Filter(ValueGetter) bool
}

type Node interface {
	Type() NodeType
	String() string
	QueryString() string
	Columns() []string
}

type ColumnNode interface {
	Node
	GetAggregateValue(*Collection, int, *TmpTable) (Value, error)
}

type NodeType int

func (n NodeType) Type() NodeType {
	return n
}

const (
	NodeText NodeType = iota // Plain text.
	NodeList
	NodeBin
	NodeIdentifier
	NodeString
	NodeNumber
	NodeBool
	NodeFunc
	NodeIdentifierList
)

// -----------------------------------------------------------------------------

var _ ValueNode = new(StringNode)

type StringNode struct {
	NodeType
	Pos int
	tr  *Tree
	Val string
}

func (t *Tree) newStringNode(i item) *StringNode {
	return &StringNode{
		NodeString, i.pos, t, i.val,
	}
}

func (s *StringNode) GetValue(ValueGetter) Value {
	return NewValue(s.Val[1 : len(s.Val)-1])
}

func (s *StringNode) Columns() []string {
	return nil
}

func (s *StringNode) String() string {
	return s.Val
}

func (s *StringNode) QueryString() string {
	return s.Val
}

// -----------------------------------------------------------------------------

var _ ValueNode = new(NumberNode)

type NumberNode struct {
	NodeType
	Pos int
	tr  *Tree
	Val int
}

func (n *NumberNode) GetValue(ValueGetter) Value {
	return NewValue(n.Val)
}

func (n *NumberNode) Columns() []string {
	return nil
}

func (n *NumberNode) String() string {
	return fmt.Sprint(n.Val)
}

func (t *Tree) newNumberNode(i item) *NumberNode {
	val, err := strconv.Atoi(i.val)
	if err != nil {
		panic(err)
	}
	return &NumberNode{
		NodeNumber, i.pos, t, val,
	}
}

func (n *NumberNode) QueryString() string {
	return fmt.Sprint(n.Val)
}

// -----------------------------------------------------------------------------

var _ ValueNode = new(BoolNode)

type BoolNode struct {
	NodeType
	Pos int
	tr  *Tree
	Val bool
}

func (b *BoolNode) GetValue(ValueGetter) Value {
	return NewValue(b.Val)
}

func (b *BoolNode) Columns() []string {
	return nil
}

func (b *BoolNode) String() string {
	return fmt.Sprint(b.Val)
}

func (t *Tree) newBoolNode(i item) *BoolNode {
	val, err := strconv.ParseBool(i.val)
	if err != nil {
		panic(err)
	}
	return &BoolNode{
		NodeBool, i.pos, t, val,
	}
}

func (n *BoolNode) QueryString() string {
	return fmt.Sprint(n.Val)
}

// -----------------------------------------------------------------------------
var _ ValueNode = new(IdentifierNode)
var _ ColumnNode = new(IdentifierNode)

type IdentifierNode struct {
	NodeType
	Pos  int
	tr   *Tree
	Name string
	idx  int
}

func (b *IdentifierNode) GetAggregateValue(col *Collection, i int, t *TmpTable) (Value, error) {
	return col.GetValue(b.Name), nil
}

func (b *IdentifierNode) SetColumnIndex(idx int) {
	b.idx = idx
}

func (b *IdentifierNode) GetValue(c ValueGetter) Value {
	if b.idx >= 0 {
		return c.GetValueByIdx(b.idx)
	}
	return c.GetValue(b.Name)
}

func (t *IdentifierNode) Columns() []string {
	return []string{t.Name}
}

func (t *IdentifierNode) String() string {
	return fmt.Sprint(t.Name)
}

func (t *Tree) newIdentifierNode(i item) *IdentifierNode {
	return &IdentifierNode{
		NodeIdentifier, i.pos, t, i.val, -1,
	}
}

func (n *IdentifierNode) QueryString() string {
	return n.Name
}

// -----------------------------------------------------------------------------

type ListNode struct {
	NodeType
	Pos   int
	tr    *Tree
	Nodes []Node // The element nodes in lexical order.
}

func NewListNode() *ListNode {
	return &ListNode{
		NodeType: NodeList,
	}
}

func (l *ListNode) Append(n Node) {
	if n == nil {
		return
	}
	l.Nodes = append(l.Nodes, n)
}

func (l *ListNode) QueryString() string {
	if len(l.Nodes) == 0 {
		return ""
	}
	buf := bytes.NewBuffer(nil)
	for _, ident := range l.Nodes {
		buf.WriteString(", " + ident.QueryString())
	}
	return buf.String()[2:]
}

func (l *ListNode) String() string {
	ret := "[\n"
	for _, node := range l.Nodes {
		ret += fmt.Sprintf("\t%v,\n", node)
	}
	return ret + "]"
}

type BinLogNode struct {
	BinNode
}

func (b *BinLogNode) LeftFilter() FilterNode  { return b.Left().(FilterNode) }
func (b *BinLogNode) RightFilter() FilterNode { return b.Right().(FilterNode) }

func (b *BinLogNode) Filter(c ValueGetter) bool {
	left := b.LeftFilter()
	right := b.RightFilter()
	switch b.Op() {
	case "and":
		return left.Filter(c) && right.Filter(c)
	case "or":
		return left.Filter(c) || right.Filter(c)
	default:
		panic("invalid op: " + b.Op())
	}
}

func (b *BinLogNode) String() string {
	return fmt.Sprintf("log{%v %v %v}", b.Left(), b.Op(), b.Right())
}

type BinCompareNode struct {
	BinNode
}

func (b *BinCompareNode) String() string {
	return fmt.Sprintf("cmp{%v %v %v}", b.Left(), b.Op(), b.Right())
}

func (b *BinCompareNode) LeftValue() ValueNode  { return b.Left().(ValueNode) }
func (b *BinCompareNode) RightValue() ValueNode { return b.Right().(ValueNode) }

func (b *BinCompareNode) Filter(c ValueGetter) bool {
	left := b.LeftValue()
	right := b.RightValue()
	switch b.Op() {
	case ">":
		return left.GetValue(c).Gt(right.GetValue(c))
	case "=":
		return left.GetValue(c).Equal(right.GetValue(c))
	case "<":
		return left.GetValue(c).Lt(right.GetValue(c))
	case ">=":
		return left.GetValue(c).Gte(right.GetValue(c))
	case "<=":
		return left.GetValue(c).Lte(right.GetValue(c))
	case "!=":
		return !left.GetValue(c).Equal(right.GetValue(c))
	case "~":
		return left.GetValue(c).Like(right.GetValue(c))
	default:
		panic("invalid type: " + b.Op())
	}
}

type BinMathNode struct {
	BinNode
}

func (b *BinMathNode) LeftValue() ValueNode  { return b.Left().(ValueNode) }
func (b *BinMathNode) RightValue() ValueNode { return b.Right().(ValueNode) }

func (b *BinMathNode) GetValue(c ValueGetter) Value {
	left := b.LeftValue().GetValue(c)
	right := b.RightValue().GetValue(c)
	switch b.Op() {
	case "+":
		return left.Plus(right)
	case "-":
		return left.Minus(right)
	case "*":
		return left.Multiply(right)
	case "/":
		return left.Divide(right)
	default:
		panic("invalid op: " + b.Op())
	}
}

func (t *BinMathNode) String() string {
	return fmt.Sprintf("math{%v %v %v}",
		t.Left(), t.Op(), t.Right())
}

type BinNode interface {
	Node
	Left() Node
	Right() Node
	Op() string
	SetGroup(bool)
}

type binNode struct {
	NodeType
	group bool
	left  Node
	right Node
	op    string
}

func (t *binNode) SetGroup(g bool) {
	t.group = g
}

func (t *binNode) Left() Node {
	return t.left
}

func (t *binNode) Op() string {
	return t.op
}

func (t *binNode) Right() Node {
	return t.right
}

func (t *binNode) String() string {
	return fmt.Sprintf("bin{%v %v %v}",
		t.left, strconv.Quote(t.op), t.right)
}

func (t *binNode) Columns() (ret []string) {
	ret = append(ret, t.Left().Columns()...)
	ret = append(ret, t.Right().Columns()...)
	return ret
}

func (t *Tree) newCmpNode(op string, left, right Node) *BinCompareNode {
	return &BinCompareNode{t.newBinNode(op, left, right)}
}

func (t *Tree) newLogNode(op string, left, right Node) *BinLogNode {
	return &BinLogNode{t.newBinNode(op, left, right)}
}

func (t *Tree) newMathNode(op string, left, right Node) *BinMathNode {
	return &BinMathNode{t.newBinNode(op, left, right)}
}

func (t *Tree) newBinNode(op string, left, right Node) BinNode {
	return &binNode{
		NodeBin, false, left, right, op,
	}
}

func (b *binNode) QueryString() string {
	op := b.op
	if op == "~" {
		op = "like"
	}
	query := b.left.QueryString() + " " + op + " " + b.right.QueryString()
	if b.group {
		return "(" + query + ")"
	}
	return query
}

// -----------------------------------------------------------------------------

var _ ColumnNode = new(FuncNode)
var _ ValueNode = new(FuncNode)

type FuncNode struct {
	NodeType
	Name   string
	Column *IdentifierNode
	idx    int
}

func (t *Tree) newFuncNode(name string, column *IdentifierNode) *FuncNode {
	return &FuncNode{
		NodeFunc, name, column, -1,
	}
}

func (f *FuncNode) GetAggregateValue(col *Collection, idx int, t *TmpTable) (Value, error) {
	switch f.Name {
	case "count":
		if t.GetRow()[idx].IsNil() {
			return NewValue(1), nil
		}
		return t.GetRow()[idx].Plus(NewValue(1)), nil
	case "sum":
		value := col.GetValue(f.Column.Name)
		row := t.GetRow()
		if row[idx].IsNil() {
			row[idx].Set(0)
		}
		return row[idx].Plus(value), nil
	default:
		return NIL, fmt.Errorf("unsupported func: %v", f.Column.Name)
	}
}

func (f *FuncNode) SetColumnIndex(idx int) {
	f.idx = idx
}

func (f *FuncNode) GetValue(c ValueGetter) Value {
	return c.GetValueByIdx(f.idx)
}

func (f *FuncNode) Columns() []string {
	if f.Column != nil {
		return f.Column.Columns()
	}
	return nil
}

func (f *FuncNode) String() string {
	return fmt.Sprintf("func{Name: %v, Column: %v}", f.Name, f.Column)
}

func (f *FuncNode) QueryString() string {
	if f.Column == nil && f.Name == "count" {
		return fmt.Sprintf("%v(*)", f.Name)
	}
	return fmt.Sprintf("%v(%v)", f.Name, f.Column)
}

// -----------------------------------------------------------------------------

type IdentifierListNode struct {
	NodeType
	List []*IdentifierNode
}

func (t *IdentifierListNode) Columns() (ret []string) {
	for _, n := range t.List {
		ret = append(ret, n.Columns()...)
	}
	return ret
}

func (i *IdentifierListNode) Append(ident *IdentifierNode) {
	i.List = append(i.List, ident)
}

func (t *Tree) newIdentifierListNode() *IdentifierListNode {
	return NewIdentifierListNode()
}

func NewIdentifierListNode() *IdentifierListNode {
	return &IdentifierListNode{
		NodeIdentifierList, nil,
	}
}

func (i *IdentifierListNode) String() string {
	return fmt.Sprint(i.List)
}

func (i *IdentifierListNode) QueryString() string {
	if len(i.List) == 0 {
		return ""
	}
	buf := bytes.NewBuffer(nil)
	for _, ident := range i.List {
		buf.WriteString(", " + ident.Name)
	}
	return buf.String()[2:]
}
