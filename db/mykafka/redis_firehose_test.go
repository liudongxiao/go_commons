package mykafka

import (
	"dmp_web/go/commons/db/redis"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestTransfer(t *testing.T) {
	rConn, err := redis.ConnectRedis("localhost", 6379)
	if err != nil {
		t.Fatal(err)
	}

	clientConf := sarama.NewConfig()
	clientConf.ClientID = "sunteng_commons_db_mykafka_test"

	clientConf.Producer.MaxMessageBytes = 1000 * 100
	clientConf.Producer.Compression = sarama.CompressionSnappy

	clientConf.Producer.Flush.Frequency = 10 * time.Second
	clientConf.Producer.Flush.MaxMessages = 100
	clientConf.Producer.Flush.Bytes = 1000 * 100

	client, err := sarama.NewClient([]string{"localhost:9092"}, clientConf)

	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	conf := &TransferConf{
		Redis:         rConn,
		RedisKey:      "JJJJJ",
		KafkaProducer: producer,
		KafkaTopic:    "redis-transfer-test-topic",
		Size:          FOREVER,
		IgnoreErr:     true,
	}
	go Transfer(conf)
	time.Sleep(10 * time.Second)
	var c = make(chan bool)
	conf.StopChan <- c
	<-c
	// fmt.Println("完美退出")
}
