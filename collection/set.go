package collection

import "k8s.io/apimachinery/pkg/util/sets"

func FlatUniqSlic(arr [][]int) []int {
	s := make(sets.Int)
	for _, a := range arr {
		s.Insert(a...)
	}
	return s.UnsortedList()
}

func UniqSliceStr(arr []string) []string {
	s := make(sets.String)
	for _, a := range arr {
		s.Insert(a)
	}
	return s.UnsortedList()
}

func UniqSliceInt(arr []int) []int {
	s := make(sets.Int)
	for _, a := range arr {
		s.Insert(a)
	}
	return s.UnsortedList()
}
