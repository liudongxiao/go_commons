package rules

import (
	"fmt"
	"testing"
)

func TestStringMatch(t *testing.T) {
	results := []struct {
		Pattern string
		Source  string
		Result  bool
	}{
		{"h%", "akjhjkj", false},
		{"h", "hjkj", false},
		{"%h", "h", true},
		{"h%", "h", true},
	}
	for idx, item := range results {
		ret := stringMatch(item.Pattern, item.Source)
		if ret != item.Result {
			t.Fatal(fmt.Sprintf("%v: '%v' ~ '%v'\n expect: %v, got: %v",
				idx, item.Source, item.Pattern, ret, item.Result))
		}
	}
}
