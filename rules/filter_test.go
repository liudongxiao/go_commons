package rules

import (
	"bufio"
	"fmt"
	"os"
	"testing"
)

func TestFilter2(t *testing.T) {
	filter := NewFilter()
	statement := `price > "20" | visitorid`
	err := filter.Process(statement)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := os.Open("testdata/query_result.json")
	if err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(fd)
	buffer := new(MapBuffer)
	for scanner.Scan() {
		out, ok, err := filter.MapJSON(buffer, scanner.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			continue
		}
		fmt.Sprint("got:", out)
	}
}

func TestFilter(t *testing.T) {
	filter := NewFilter()
	statement := `advid = "34828" | distinct(ostype) | sum(ostype) > 50000 | advid`
	statement = `advid = "34828"  | visitorid`
	err := filter.Process(statement)
	if err != nil {
		t.Fatal(err)
	}

	col, err := filter.ReadCSVFile("testdata/query_result.csv")
	if err != nil {
		t.Fatal(err)
	}

	tbl := filter.GetResult(col)

	tbl.Reset()
	for tbl.Next() {
		fmt.Println(tbl.GetRow())
	}

}
