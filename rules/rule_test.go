package rules

import "testing"

func TestRule(t *testing.T) {
	in := [][2]string{
		{
			`a = (1 + (2 * 3))  | userId`,
			"SELECT userId FROM table WHERE a = 1 + (2 * 3)",
		}, {
			`(((adv="a" or d="2")) and c="d") | userId`,
			`SELECT userId FROM table WHERE (adv = "a" or d = "2") and c = "d"`,
		}, {
			`adv = "xxx" | count()>23 | userId`,
			`SELECT userId FROM table WHERE adv = "xxx" HAVING count(*) > 23`,
		}, {
			`adv="xxx" and create>10 | distinct(素材) | count() = 124 | userId`,
			`SELECT userId FROM table WHERE adv = "xxx" and create > 10 GROUP BY 素材, userId HAVING count(*) = 124`,
		}, {
			`distinct(userId)`,
			`SELECT userId FROM table GROUP BY userId`,
		}, {
			`adv = "xxx" | distinct(userId)`,
			`SELECT userId FROM table WHERE adv = "xxx" GROUP BY userId`,
		}, {
			`url ~ "host/%" | distinct(userId)`,
			`SELECT userId FROM table WHERE url like "host/%" GROUP BY userId`,
		}, {
			`adv = "xxx" | userId,adv`,
			`SELECT userId, adv FROM table WHERE adv = "xxx"`,
		}, {
			`advid = "35635" | distinct(advid) | count() > 3 | visitorId`,
			`SELECT visitorId FROM table WHERE advid = "35635" GROUP BY advid, visitorId HAVING count(*) > 3`,
		}, {
			`advid = "123" | visitor.id`,
			`SELECT visitor.id FROM table WHERE advid = "123"`,
		},
	}

	for _, i := range in {
		ret, err := Parse("table", i[0])
		if err != nil {
			t.Error(i[0] + "\n" + err.Error())
		}
		if ret != i[1] {
			t.Error("\nwant:\n", i[1], "\ngot:\n", ret)
		}
	}
}
