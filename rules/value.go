package rules

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type Kind int

const (
	Int Kind = iota
	String
	Float
	Number
	Invalid
)

var NIL = Value{}

type Value struct {
	val interface{}
}

func NewRawValue(obj interface{}) Value {
	return Value{obj}
}

func NewValue(obj interface{}) Value {
	switch n := obj.(type) {
	case int:
		obj = int64(n)
	case json.Number:
		ret, err := n.Int64()
		if err == nil {
			obj = ret
		} else {
			ret, err := n.Float64()
			if err == nil {
				obj = ret
			} else {
				obj = string(n)
			}
		}
	}
	return Value{obj}
}

func (s Value) Int() int64 {
	return s.val.(int64)
}
func (s Value) Float() float64 {
	return s.val.(float64)
}
func (s Value) Bytes() []byte {
	if n, ok := s.val.(float64); ok {
		s.val = int64(n)
	}
	return []byte(fmt.Sprint(s.val))
}

func (s Value) getKind() Kind {
	switch s.val.(type) {
	case int64:
		return Int
	case string, json.Number:
		return String
	case float64:
		return Float
	default:
		return Invalid
	}
}

func castingIntToFloat(intValue *Value, floatValue *Value) {
	intValue.val = float64(intValue.val.(int64))
}

func castingFloatString(float *Value, str *Value) {
	strToFloat, err := strconv.ParseFloat(str.Str(), 64)
	if err != nil {
		panic(err)
	}
	str.Set(strToFloat)
}

func castingIntString(intValue *Value, strValue *Value) {
	strToInt, err := strconv.ParseInt(strValue.val.(string), 10, 64)
	if err != nil {
		// parse int to string
		intToStr := strconv.FormatInt(intValue.Int(), 10)
		intValue.val = intToStr
		return
	}
	strValue.val = strToInt
}

func (s *Value) casting(s2 *Value) {
	k1 := s.getKind()
	k2 := s2.getKind()
	switch {
	case k1 == Float && k2 == Int:
		castingIntToFloat(s2, s)
	case k2 == Float && k1 == Int:
		castingIntToFloat(s, s2)
	case k1 == Int && k2 == String:
		castingIntString(s, s2)
	case k2 == Int && k1 == String:
		castingIntString(s2, s)
	case k1 == Float && k2 == String:
		castingFloatString(s, s2)
	case k2 == Float && k1 == String:
		castingFloatString(s2, s)
	}
}

func (s Value) Str() string {
	switch n := s.val.(type) {
	case json.Number:
		return n.String()
	case string:
		return s.val.(string)
	case int:
		return strconv.Itoa(n)
	case int64:
		return strconv.FormatInt(n, 10)
	case float64:
		return strconv.FormatFloat(n, 'f', -1, 64)
	default:
		panic(fmt.Sprintf("not reach: %+T", s.val))
	}
}

func (s Value) String() string {
	return fmt.Sprintf("%+T(%v)", s.val, s.val)
}

func (v Value) IsNil() bool {
	return v.val == nil
}

func (v *Value) Set(o interface{}) {
	v.val = o
}

func (v Value) Plus(v2 Value) Value {
	v.casting(&v2)
	switch v.val.(type) {
	case int:
		return NewValue(v.Int() + v2.Int())
	case string:
		return NewValue(v.Str() + v2.Str())
	default:
		panic(fmt.Sprintf("unsupport type, %+T", v.val))
	}
}

func (v Value) Minus(v2 Value) Value {
	v.casting(&v2)
	switch v.val.(type) {
	case int:
		return NewValue(v.Int() - v2.Int())
	default:
		panic(fmt.Sprintf("unsupport type, %+T", v.val))
	}
}

func (v Value) Multiply(v2 Value) Value {
	v.casting(&v2)
	switch v.val.(type) {
	case int:
		return NewValue(v.val.(int) * v2.val.(int))
	default:
		panic(fmt.Sprintf("unsupport type, %+T", v.val))
	}
}

func (v Value) Divide(v2 Value) Value {
	v.casting(&v2)
	switch v.val.(type) {
	case int:
		return NewValue(v.val.(int) / v2.val.(int))
	default:
		panic(fmt.Sprintf("unsupport type, %+T", v.val))
	}
	return v
}

func (v Value) Equal(v2 Value) bool {
	return v.Cmp(v2) == 0
}

func (v Value) Like(v2 Value) bool {
	if v.getKind() != String {
		return false
	}
	if v2.getKind() != String {
		return false
	}
	return stringMatch(v2.val.(string), v.val.(string))
}

func (v Value) Cmp(v2 Value) (ret int) {
	v.casting(&v2)
	switch v.getKind() {
	case Float:
		vflt1 := v.Float()
		vflt2 := v2.Float()
		if vflt1 == vflt2 {
			return 0
		} else if vflt1 > vflt2 {
			return 1
		} else {
			return -1
		}
	case Int:
		vint1 := v.Int()
		vint2 := v2.Int()
		if vint1 == vint2 {
			return 0
		} else if vint1 > vint2 {
			return 1
		} else {
			return -1
		}
	case String:
		if v2.IsNil() {
			return 1
		}
		vstr1 := v.Str()
		vstr2 := v2.Str()
		if vstr1 == vstr2 {
			return 0
		} else if vstr1 > vstr2 {
			return 1
		} else {
			return -1
		}
	default:
		panic(fmt.Sprintf("unsupport type, %+T", v.val))
	}
}

func (v Value) Gt(v2 Value) bool {
	return v.Cmp(v2) == 1
}

func (v Value) Lt(v2 Value) bool {
	return v.Cmp(v2) == -1
}

func (v Value) Gte(v2 Value) bool {
	switch v.Cmp(v2) {
	case 0, 1:
		return true
	default:
		return false
	}
}

func (v Value) Lte(v2 Value) bool {
	switch v.Cmp(v2) {
	case 0, -1:
		return true
	default:
		return false
	}
}
