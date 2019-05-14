package day

import (
	"dmp_web/go/commons/env"
	"dmp_web/go/commons/errors"
	"strings"
	"time"
)

func GetDayRange(date string) ([]string, error) {
	dayrange := strings.Split(date, ",")
	if len(dayrange) == 1 {
		return []string{dayrange[0]}, nil
	}
	if len(dayrange) != 2 {
		return nil, errors.Wrap(errors.ErrDay{date}, "")
	}
	from := dayrange[0]
	to := dayrange[1]
	toTime, err := time.Parse(env.DayFormat, to)
	if err != nil {
		return nil, err
	}
	fromTime, err := time.Parse(env.DayFormat, from)
	if err != nil {
		return nil, err
	}
	numDay := (int(toTime.Sub(fromTime).Hours() / 24))
	if numDay < 0 {
		return nil, errors.Wrap(errors.ErrDay{date}, "")
	}

	res := make([]string, 0, numDay)
	for i := numDay; i >= 0; i-- {
		res = append(res, fromTime.AddDate(0, 0, i).Format(env.DayFormat))
	}
	return res, nil
}
