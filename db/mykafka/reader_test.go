package mykafka

import (
	"fmt"

	"github.com/Shopify/sarama"
	. "github.com/smartystreets/goconvey/convey"

	"testing"
	"time"
)

func TestConsumer(t *testing.T) {
	Convey("测试 kafka 读取", t, func() {
		return
		gtConf := GroupTopicConfig{
			Group:     "TestKafkaImporterGroup",
			Topic:     "TestKafkaImporterTopic",
			ThreadNum: 5,

			Kafka:     []string{"localhost:9092"},
			Zookeeper: []string{"localhost"},
			ZkChroot:  "/kafkatest",
		}
		gtConsumer, err := NewGroupTopicConsumer(gtConf)
		So(err, ShouldBeNil)
		defer gtConsumer.Close()
		for i, ch := range gtConsumer.MsgChans() {
			go func(i int, ch <-chan *sarama.ConsumerMessage) {
				for {
					v, ok := <-ch
					if !ok {
						fmt.Println(i, "Chan 已关闭")
						break
					}
					fmt.Println(i, ": ", v)
				}
			}(i, ch)
		}
		time.Sleep(time.Second * 3)
		fmt.Println("OVER++++++++++")
	})
}
