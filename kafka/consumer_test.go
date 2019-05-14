package kafka

import (
	"fmt"
	"testing"
)

func TestConsumer(t *testing.T) {
	return
	c, err := NewConsumer(&Config{
		Kafka:     []string{"sf51:9092"},
		Topic:     "dsp_masky_ana_mq_log",
		GroupName: "test",
		Partition: []int32{0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

loop:
	for {
		select {
		case msg := <-c.Message():
			fmt.Printf("%+v\n", msg)
			c.Commit(msg)
			break
		case err := <-c.Error():
			t.Fatal(err)
			break loop
		}
	}
}
