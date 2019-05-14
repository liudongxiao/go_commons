package rules

import (
	"fmt"
	"strings"
)

type Tree struct {
	Root        *ListNode
	parentDepth int

	token     [3]item
	peekCount int
	lex       *lexer
}

func NewTree() *Tree {
	return &Tree{}
}

func (t *Tree) Parse(text string) {
	t.lex = newLexer(text)
	t.Root = t.newList(t.peekNonSpace().pos)
	for t.peekNonSpace().typ != itemEOF {
		switch t.peek().typ {
		case itemLeftParen:
			t.Root.Append(t.whereExpr())
		case itemIdentifier:
			p := t.next()
			switch t.peekNonSpace().typ {
			case itemLeftParen:
				t.backup2(p)
				t.Root.Append(t.groupOrHavingExpr())
			case itemRelOp:
				t.backup2(p)
				t.Root.Append(t.whereExpr())
			case itemEOF, itemComma:
				t.backup2(p)
				t.Root.Append(t.selectExpr())
			default:
				t.errorf("unexpect type: %v", p.typ)
				return
			}
		case itemPipe:
			t.next() // just consume it
		default:
			t.errorf("unexpect type: %v", t.peek().typ)
			return
		}
	}
}

func (t *Tree) value() (ret ValueNode) {
	switch n := t.nextNonSpace(); n.typ {
	case itemString:
		return t.newStringNode(n)
	case itemBool:
		return t.newBoolNode(n)
	case itemNumber:
		return t.newNumberNode(n)
	default:
		return t.errorf("unknown type: %v", n.typ)
	}
}

func (t *Tree) valueExpr() (ret ValueNode) {
	first := t.peekNonSpace()
	if first.typ == itemLeftParen {
		t.parentDepth++
		t.next()
		defer func() {
			if t.parentDepth > 0 {
				if t.peekNonSpace().typ == itemRightParen {
					t.next()
					t.parentDepth--
					if ret == nil {
						return
					}
					if bn, ok := ret.(BinNode); ok {
						bn.SetGroup(true)
					}
				} else {
					t.errorf("missing )")
				}
			}
		}()
	}

	var left, right ValueNode
	switch t.peekNonSpace().typ {
	case itemLeftParen:
		left = t.valueExpr()
	default:
		left = t.value()
	}

loop:
	for {
		switch t.peekNonSpace().typ {
		case itemRightParen:
			// defer will handle it
			break loop
		case itemMath:
		case itemPipe, itemEOF:
			break loop
		default:
			break loop
		}

		rel := t.next()

		switch t.peekNonSpace().typ {
		case itemLeftParen:
			right = t.valueExpr()
		default:
			right = t.value()
		}
		left = t.newMathNode(rel.val, left, right)
	}

	ret = left
	return
}

func (t *Tree) selectExpr() Node {
	list := t.newIdentifierListNode()
	for {
		ident := t.nextNonSpace()
		if ident.typ != itemIdentifier {
			return t.errorf(
				"unexpected token: %v, want: %v", ident.typ, itemIdentifier)
		}
		list.Append(t.newIdentifierNode(ident))
		p := t.peekNonSpace()
		if p.typ == itemComma {
			t.next()
			continue
		}
		break
	}
	return list
}

func (t *Tree) checkRightParen() bool {
	if t.parentDepth > 0 && t.peekNonSpace().typ == itemRightParen {
		t.next()
		return true
	}
	return false
}

func (t *Tree) relExpr() (ret *BinCompareNode) {
	identitfer := t.nextNonSpace()
	t.expect(identitfer, itemIdentifier)
	rel := t.nextNonSpace()
	t.expect(rel, itemRelOp)
	right := t.valueExpr()
	// ignore top level brackets
	if bn, ok := right.(BinNode); ok {
		bn.SetGroup(false)
	}
	return t.newCmpNode(rel.val, t.newIdentifierNode(identitfer), right)
}

// a = b and c = d
func (t *Tree) whereExpr() (ret FilterNode) {
	first := t.peekNonSpace()
	if first.typ == itemLeftParen {
		t.parentDepth++
		t.next()
		defer func() {
			if ret == nil {
				return
			}
			if t.parentDepth > 0 {
				if t.peekNonSpace().typ == itemRightParen {
					t.parentDepth--
					t.next()
					ret.(BinNode).SetGroup(true)
				} else {
					t.errorf("missing )")
				}
			}
		}()
	}

	var left, right FilterNode
	switch t.peekNonSpace().typ {
	case itemLeftParen:
		left = t.whereExpr()
	default:
		left = t.relExpr()
	}

loop:
	for {
		switch t.peekNonSpace().typ {
		case itemRightParen:
			// let defer handle it
			break loop
		case itemEOF, itemPipe:
			break loop
		case itemBinLogic:
		default:
			t.errorf("unexpect type: %v, %v", t.peekNonSpace(), left)
		}

		op := t.next()
		switch t.peekNonSpace().typ {
		case itemLeftParen:
			right = t.whereExpr()
		default:
			right = t.relExpr()
		}
		left = t.newLogNode(op.val, left, right)
	}
	ret = left
	return
}

func (t *Tree) expect(i item, it itemType) {
	if i.typ == it {
		return
	}
	t.errorf("unexpected type: %v, want: %v", i.typ, it)
}

func (t *Tree) groupOrHavingExpr() Node {
	funcNode := t.groupExpr()

	switch p := t.peekNonSpace(); p.typ {
	case itemRelOp:
		return t.havingExpr(funcNode.(*FuncNode))
	case itemPipe, itemEOF:
		return funcNode
	default:
		return t.errorf("kdjfkdjf")
	}

}

func (t *Tree) groupExpr() Node {
	funcName := t.nextNonSpace()

	if p := t.nextNonSpace(); p.typ != itemLeftParen {
		return t.errorf(
			"unexpected type: %v, want %v", p.typ, itemLeftParen)
	}

	var column *IdentifierNode
	switch p := t.peekNonSpace(); p.typ {
	case itemRightParen:

	case itemIdentifier:
		column = t.newIdentifierNode(p)
		t.next()
	default:
		return t.errorf("unknown type: %v", p.typ)
	}

	if typ := t.nextNonSpace().typ; typ != itemRightParen {
		return t.errorf("unexpected type: %v", typ)
	}

	// only count(*) is accpeted
	if column == nil && !strings.EqualFold(funcName.val, "count") {
		return t.errorf("function call of %v() need argument", funcName.val)
	}

	return t.newFuncNode(funcName.val, column)
}

func (t *Tree) havingExpr(f *FuncNode) Node {
	op := t.nextNonSpace()
	value := t.valueExpr()
	return t.newCmpNode(op.val, f, value)
}

// -----------------------------------------------------------------------------

func (t *Tree) newList(pos int) *ListNode {
	return &ListNode{tr: t, NodeType: NodeList, Pos: pos}
}

// -----------------------------------------------------------------------------

func (t *Tree) debug() {
	fmt.Printf("Token: %+v\nPeekCount: %v\n", t.token, t.peekCount)
}

func (t *Tree) backup() {
	t.peekCount++
}

func (t *Tree) backup2(i item) {
	t.token[1] = i
	t.peekCount = 2
}

func (t *Tree) backup3(t2, t1 item) {
	t.token[1] = t1
	t.token[2] = t2
	t.peekCount = 3
}

func (t *Tree) next() item {
	if t.peekCount > 0 {
		t.peekCount--
	} else {
		t.token[t.peekCount] = t.lex.nextItem()
	}
	return t.token[t.peekCount]
}

func (t *Tree) nextNonSpace() (token item) {
	for {
		token = t.next()
		if token.typ != itemSpace {
			break
		}
	}
	return token
}

func (t *Tree) peek() item {
	if t.peekCount > 0 {
		return t.token[t.peekCount-1]
	}
	t.peekCount = 1
	t.token[0] = t.lex.nextItem()
	return t.token[0]
}

func (t *Tree) peekNonSpace() (token item) {
	for {
		token = t.next()
		if token.typ != itemSpace {
			break
		}
	}
	t.backup()
	return token
}

func (t *Tree) errorf(format string, obj ...interface{}) *dumpNode {
	panic(fmt.Sprintf("parse error: "+format, obj...))
	return nil
}

type dumpNode struct{}

func (dumpNode) Columns() []string          { return nil }
func (dumpNode) GetValue(ValueGetter) Value { return NIL }
func (dumpNode) QueryString() string        { return "" }
func (dumpNode) String() string             { return "" }
func (dumpNode) Type() NodeType             { return -1 }
