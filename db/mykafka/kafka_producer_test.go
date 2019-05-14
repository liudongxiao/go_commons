package mykafka

import (
	"strconv"

	. "github.com/smartystreets/goconvey/convey"

	"testing"
)

func TestKafkaImporter(t *testing.T) {
	Convey("测试 kafka importer", t, func() {
		importer, err := NewKafkaImporter("TestKafkaImporterClient2", []string{"localhost:9092"}, nil)
		So(err, ShouldBeNil)
		for i := 0; i < 360; i++ {
			importer.Save("TestKafkaImporterTopic2", []byte("VVVVXXXX"+strconv.Itoa(i)))
		}
		importer.Exit()
	})

}
