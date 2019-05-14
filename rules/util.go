package rules

import "strings"

func stringsIdx(s string, col []string) int {
	for idx, item := range col {
		if s == item {
			return idx
		}
	}
	return -1
}

// TODO: just implement prefix matching ans suffix matching
func stringMatch(pattern, source string) bool {
	sp := strings.Split(pattern, "%")
	if len(sp) > 2 {
		// just implement prefix matching ans suffix matching
		return false
	}
	if len(sp) == 1 {
		return pattern == source
	}
	return strings.HasPrefix(source, sp[0]) && strings.HasSuffix(source, sp[1])
}
