package hbase

import (
	"fmt"
	"net"
	"testing"

	"git.apache.org/thrift.git/lib/go/thrift"
	. "github.com/smartystreets/goconvey/convey"
)

func TestHbase(t *testing.T) {
	Convey("TestHbase", t, func() {
		trans, err := thrift.NewTSocket(net.JoinHostPort("sf42", "9090"))
		So(err, ShouldEqual, nil)
		defer trans.Close()
		client := NewTHBaseServiceClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		err = trans.Open()
		So(err, ShouldEqual, nil)
		printRes(client.Get([]byte("test"), &TGet{
			Row: []byte("row1"),
		}))
	})
}

func printRes(res *TResult_, err error) {
	So(err, ShouldEqual, nil)

	fmt.Println("row:", string(res.GetRow()))
	for i, v := range res.GetColumnValues() {
		fmt.Printf("%d : %s\n", i, colToStr(v))
	}
}

func colToStr(c *TColumnValue) string {
	return fmt.Sprintf("Family: %s, Qualifier: %s, Value: %s, Timestamp: %d, Tags: %s", string(c.Family), string(c.Qualifier), string(c.Value), *c.Timestamp, string(c.Tags))
}
