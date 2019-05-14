package mykafka

import (
	"dmp_web/go/commons/log"
	"errors"
	"time"

	"github.com/Shopify/sarama"
	"gopkg.in/redis.v4"
)

// 从 redis 的队列获取数据然后放进 Kafka 队列的辅助方法
type TransferConf struct {
	Redis         *redis.Client
	RedisKey      string
	KafkaProducer sarama.AsyncProducer
	KafkaTopic    string
	Size          int64
	IgnoreErr     bool
	IdleSleepSec  int64
	StopChan      chan chan bool
}

const FOREVER = -999
const NILERR = "(nil)"
const DEF_SLEEP_SEC = 1

func Transfer(conf *TransferConf) (err error) {
	size := conf.Size
	if size == 0 {
		return errors.New("TransferConf配置中Size不能为0")
	}
	redisConn := conf.Redis
	kafkaConn := conf.KafkaProducer
	runForever := conf.Size == FOREVER
	if conf.StopChan == nil {
		conf.StopChan = make(chan chan bool, 1)
	}
	sleepSec := conf.IdleSleepSec
	if sleepSec == 0 {
		sleepSec = DEF_SLEEP_SEC
	}

	var popRes *redis.StringCmd
	for runForever || size > 0 {
		select {
		case c := <-conf.StopChan:
			log.Debugf(conf.KafkaTopic + "收到退出信息,正常退出")
			c <- true
			return nil
		default:
		}
		popRes = redisConn.RPop(conf.RedisKey)
		if err = popRes.Err(); err == nil {
			//插入到 kafka 中
			select {
			case e := <-kafkaConn.Errors():
				if e != nil && e.Err != nil {
					err = e.Err
				}
			case kafkaConn.Input() <- &sarama.ProducerMessage{Topic: conf.KafkaTopic, Value: sarama.StringEncoder(popRes.Val())}:
				log.Debugf(conf.KafkaTopic+"成功获取了信息 [%s] 并放到Kafka中", popRes.Val())
				if !runForever {
					size--
				}
				continue
			}
		}
		if err.Error() == NILERR {
			log.Debugf(conf.KafkaTopic+"空队列,睡眠 %d 秒", sleepSec)
			time.Sleep(time.Duration(sleepSec) * time.Second)
			continue
		}
		if !conf.IgnoreErr {
			return err
		} else {
			log.Warnf(conf.KafkaTopic+"错误:%s", err.Error())
		}
	}
	return
}
