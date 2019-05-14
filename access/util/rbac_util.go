package util

import "dmp_web/go/commons/access/models"

func UnqiInt64Slice(ids []int64) []int64 {
	if len(ids) <= 1 {
		return ids
	}
	p := 0
	m := make(map[int64]bool)
	for _, id := range ids {
		if ok := m[id]; !ok {
			m[id] = true
			ids[p] = id
			p++
		}
	}
	return ids[:p]
}

func UniqFuncs(funcs []*models.Functions) []*models.Functions {
	if len(funcs) <= 1 {
		return funcs
	}
	p := 0
	m := make(map[int64]bool)
	for _, priv := range funcs {
		if ok := m[priv.Id]; !ok {
			m[priv.Id] = true
			funcs[p] = priv
			p++
		}
	}
	return funcs[:p]
}

func ContainsFunctions(functions []*models.Functions, funcIds ...int64) bool {
	var v = make(map[int64]bool)
	for _, function := range functions {
		v[function.Id] = true
	}
	for _, id := range funcIds {
		if !v[id] {
			return false
		}
	}
	return true
}

func ContainsAnyFunction(functions []*models.Functions, funcIds ...int64) bool {
	var v = make(map[int64]bool)
	for _, function := range functions {
		v[function.Id] = true
	}
	for _, id := range funcIds {
		if v[id] {
			return true
		}
	}
	return false
}

func Contains(res []string, ps ...string) bool {
	var mp = make(map[string]bool)
	for _, str := range res {
		mp[str] = true
	}
	for _, p := range ps {
		var flag = mp[p]
		if !flag {
			return false
		}
	}
	return true

}

func ContainsAny(res []string, ps ...string) bool {
	var mp = make(map[string]bool)
	for _, str := range res {
		mp[str] = true
	}
	for _, p := range ps {
		var flag = mp[p]
		if flag {
			return true
		}
	}
	return false

}
