package rules

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

// -----------------------------------------------------------------------------

func Parse(table string, i string) (ret string, err error) {
	defer func() {
		if err != nil {
			return
		}
		p := recover()
		if p != nil {
			perr := fmt.Errorf("%v", p)
			if strings.HasPrefix(perr.Error(), "parse error:") {
				err = perr
			} else {
				panic(p)
			}
		}
	}()
	tree := NewTree()
	tree.Parse(i)
	ret, err = newParse().Run(table, tree)
	return
}

type parse struct {
	t *Tree
}

func newParse() *parse {
	return &parse{}
}

func (p *parse) isGroupNode(n Node) bool {
	f, ok := n.(*FuncNode)
	if !ok {
		return false
	}
	if !strings.EqualFold(f.Name, "distinct") {
		return false
	}
	return true
}

func (p *parse) isHavingNode(n Node) bool {
	switch n.(type) {
	case FilterNode:
		return true
	default:
		return false
	}
}

func (p *parse) isSelectNode(n Node) bool {
	switch n.(type) {
	case *IdentifierListNode:
		return true
	}
	return false
}

func (p *parse) isWhereNode(n Node) bool {
	f, ok := n.(FilterNode)
	if !ok {
		return false
	}
	var node Node
	for {
		if node == nil {
			node = f.Left()
		} else if node == f.Left() {
			node = f.Right()
		} else {
			return true
		}

		if bn, ok := node.(FilterNode); ok {
			if !p.isWhereNode(bn) {
				return false
			}
		} else {
			switch node.(type) {
			case *FuncNode:
				return false
			}
		}
	}
}

// where | group | having | select
// where | select
func (p *parse) Parse(tree *Tree) (*Query, error) {
	listNode := tree.Root
	q := NewQuery()
	if len(listNode.Nodes) > 4 {
		return nil, errors.New("the number of elements can't be more than 4")
	}
	const (
		where = iota
		group
		having
		selection
	)
	var state = where
	nodes := listNode.Nodes
loop:
	for len(nodes) > 0 {
		switch state {
		case where:
			if p.isWhereNode(nodes[0]) {
				q.Where = nodes[0].(FilterNode)
				if bn, ok := nodes[0].(BinNode); ok {
					bn.SetGroup(false)
					nodes = nodes[1:]
				}
			}
		case group:
			if p.isGroupNode(nodes[0]) {
				q.Group = NewIdentifierListNode()
				q.Group.Append(nodes[0].(*FuncNode).Column)
				nodes = nodes[1:]
			}
		case having:
			if p.isHavingNode(nodes[0]) {
				q.Having = nodes[0].(FilterNode)
				nodes = nodes[1:]
			}
		case selection:
			if p.isSelectNode(nodes[0]) {
				identList := nodes[0].(*IdentifierListNode)
				q.Select = identList
				if q.Group != nil {
					for _, ident := range identList.List {
						q.Group.Append(ident)
					}
				}
				nodes = nodes[1:]
			}
			break loop
		default:
			break loop
		}
		state++
	}

	if q.Select == nil && q.Group != nil {
		q.Select = NewIdentifierListNode()
		for _, g := range q.Group.List {
			q.Select.Append(g)
		}
	}

	return q, nil
}

func (p *parse) Run(tbl string, tree *Tree) (string, error) {
	q, err := p.Parse(tree)
	if err != nil {
		return "", err
	}
	return q.QueryString(tbl), nil
}

type Query struct {
	Select *IdentifierListNode
	Group  *IdentifierListNode
	Having FilterNode
	Where  FilterNode
}

func NewQuery() *Query {
	return &Query{}
}

func (q *Query) GetWhereIdents() (ret []*IdentifierNode) {
	checkList := []FilterNode{q.Where}
	check := func(n Node) {
		switch n := n.(type) {
		case *IdentifierNode:
			ret = append(ret, n)
		case FilterNode:
			checkList = append(checkList, n)
		}
	}

	for len(checkList) > 0 {
		fn := checkList[len(checkList)-1]
		checkList = checkList[:len(checkList)-1]
		if fn == nil {
			continue
		}
		check(fn.Left())
		check(fn.Right())
	}
	return
}

func (q *Query) GetHavingFuncs() (ret []*FuncNode) {
	checkList := []FilterNode{q.Having}
	check := func(n Node) {
		switch n := n.(type) {
		case *FuncNode:
			ret = append(ret, n)
		case FilterNode:
			checkList = append(checkList, n)
		default:
		}
	}
	for len(checkList) > 0 {
		fn := checkList[len(checkList)-1]
		checkList = checkList[:len(checkList)-1]
		if fn == nil {
			continue
		}
		check(fn.Left())
		check(fn.Right())
	}
	return
}

func (q *Query) GroupString() string {
	return q.Group.QueryString()
}

func (q *Query) QueryString(tbl string) string {
	buf := bytes.NewBuffer(nil)

	// SELECT * FROM {table}
	buf.WriteString("SELECT ")
	buf.WriteString(q.Select.QueryString())
	buf.WriteString(" FROM " + tbl)

	if q.Where != nil {
		buf.WriteString(" WHERE " + q.Where.QueryString())
	}
	if q.Group != nil {
		buf.WriteString(" GROUP BY " + q.GroupString())
	}
	if q.Having != nil {
		buf.WriteString(" HAVING " + q.Having.QueryString())
	}
	return buf.String()
}
