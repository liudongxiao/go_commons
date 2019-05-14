package rules

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// -----------------------------------------------------------------------------

const (
	itemEOF itemType = iota
	itemError
	itemPipe
	itemLeftParen
	itemRightParen
	itemChar
	itemSpace
	itemString
	itemRelOp
	itemIdentifier
	itemComma
	itemNumber
	itemBinLogic
	itemBool
	itemMath
)

func (i itemType) String() string {
	switch i {
	case itemEOF:
		return "eof"
	case itemError:
		return "error"
	case itemPipe:
		return "pipe"
	case itemLeftParen:
		return "leftParen"
	case itemRightParen:
		return "rightParen"
	case itemChar:
		return "char"
	case itemSpace:
		return "space"
	case itemString:
		return "string"
	case itemRelOp:
		return "relop"
	case itemIdentifier:
		return "identifier"
	case itemComma:
		return "comma"
	case itemNumber:
		return "number"
	case itemBinLogic:
		return "binLogic"
	case itemBool:
		return "bool"
	case itemMath:
		return "math"
	default:
		return "unknown"
	}
}

func isOneOf(s string, r rune) bool {
	return strings.IndexRune(s, r) >= 0
}

func isSpace(r rune) bool {
	return isOneOf(" \t\r", r)
}

func isAlphaNumberic(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r)
}

// -----------------------------------------------------------------------------

func lexIdentifier(l *lexer) stateFn {
loop:
	for {
		switch r := l.next(); {
		case isAlphaNumberic(r):
		case r == '.':
		default:
			l.backup()
			break loop
		}
	}
	switch l.stage() {
	case "and", "or", "AND", "OR":
		l.emit(itemBinLogic)
	case "true", "false":
		l.emit(itemBool)
	default:
		l.emit(itemIdentifier)
	}
	return lexStartState
}

func lexBracket(l *lexer) stateFn {
	switch l.next() {
	case '(':
		l.parentDepth++
		l.emit(itemLeftParen)
	case ')':
		l.parentDepth--
		if l.parentDepth < 0 {
			return l.errorf("unclosed bracket")
		}
		l.emit(itemRightParen)
	}
	return lexStartState
}

func lexLogic(l *lexer) stateFn {
	switch l.next() {
	case '&':
		if !l.accpet("&") {
			return l.errorf("& missing &")
		}
	case '|':
		if !l.accpet("|") {
			return l.errorf("| missing |")
		}
	}
	return lexStartState
}

func lexStartState(l *lexer) stateFn {
	switch r := l.next(); {
	case r == eof:
		if l.parentDepth < 0 {
			return l.errorf("unclosed bracket")
		}
		return nil
	case isSpace(r):
		return lexSpace
	case r == '|':
		l.emit(itemPipe)
	case isOneOf("+-*/", r):
		l.emit(itemMath)
	case isOneOf("<=>~!", r):
		l.backup()
		return lexOp
	case r == '(' || r == ')':
		l.backup()
		return lexBracket
	case r == ',':
		l.emit(itemComma)
	case r == '"':
		return lexQuote
	case isAlphaNumberic(r):
		if unicode.IsNumber(r) {
			return lexNumber
		}
		return lexIdentifier
	case r < unicode.MaxASCII && unicode.IsPrint(r):
		l.emit(itemChar)
	}
	return lexStartState
}

func lexNumber(l *lexer) stateFn {
loop:
	for {
		switch r := l.next(); {
		case unicode.IsNumber(r):
		case r == '.':
			l.accpetAll("0123456789")
		default:
			l.backup()
			break loop
		}
	}
	l.emit(itemNumber)
	return lexStartState
}

func lexOp(l *lexer) stateFn {
	switch l.next() {
	case '!':
		if !l.accpet("=") {
			return l.errorf("! missing =")
		}
	case '=':
	case '>':
		l.accpet("=")
	case '<':
		l.accpet("=")
	case '~':
	default:
		return l.errorf("unknown op")
	}
	l.emit(itemRelOp)
	return lexStartState
}

func lexQuote(l *lexer) stateFn {
loop:
	for {
		switch l.next() {
		case '\\':
			if r := l.next(); r != eof {
				break
			}
			fallthrough
		case eof:
			return l.errorf("uniterminated quoted string")
		case '"':
			break loop
		}
	}
	l.emit(itemString)
	return lexStartState
}

func lexSpace(l *lexer) stateFn {
	for isSpace(l.peek()) {
		l.next()
	}
	l.emit(itemSpace)
	return lexStartState
}

// -----------------------------------------------------------------------------

type stateFn func(*lexer) stateFn

const (
	eof = 0
)

type itemType int

type Pos int

type item struct {
	typ itemType
	pos int
	val string
}

type lexer struct {
	input       string
	pos         int
	start       int
	width       int
	parentDepth int

	state stateFn
	items chan item
}

func newLexer(input string) *lexer {
	return &lexer{
		input: input,
		state: lexStartState,
		items: make(chan item, 2),
	}
}

func (l *lexer) nextItem() item {
	for {
		select {
		case item := <-l.items:
			return item
		default:
			if l.state == nil {
				return item{typ: itemEOF}
			}
			l.state = l.state(l)
		}
	}
}

func (l *lexer) next() rune {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}
	rune, width := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = width
	l.pos += width
	return rune
}

func (l *lexer) peek() rune {
	rune := l.next()
	l.backup()
	return rune
}

func (l *lexer) backup() {
	l.pos -= l.width
	l.width = 0
}

func (l *lexer) accpet(valid string) bool {
	if strings.IndexRune(valid, l.next()) >= 0 {
		return true
	}
	l.backup()
	return false
}

func (l *lexer) accpetAll(valid string) {
	for strings.IndexRune(valid, l.next()) >= 0 {
	}
	l.backup()
}

func (l *lexer) errorf(format string, args ...interface{}) stateFn {
	l.items <- item{
		typ: itemError,
		val: fmt.Sprintf(format, args...),
	}
	return nil
}

func (l *lexer) emit(typ itemType) {
	l.items <- item{
		typ: typ,
		pos: l.start,
		val: l.input[l.start:l.pos],
	}
	l.start = l.pos
}

func (l *lexer) ignore() {
	l.start = l.pos
}

func (l *lexer) stage() string {
	return l.input[l.start:l.pos]
}

func (l *lexer) ignoreAll(s string) {
	l.accpetAll(s)
	l.ignore()
}
