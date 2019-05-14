// Usage:
//  func main() {
//    consumer, err := NewConsumer(cfg)
//    if err != nil { panic(err) }
//    defer consumer.Close()
//    signal := make(os.Signal)
//  loop:
//    for {
//      select {
//      case msg := <-consumer.Message():
//        // handle message
//        consumer.Commit(msg)
//      case err := <-consumer.Error():
//        // handle error
//        break loop
//      case <-signal:
//        break loop
//      }
//    }
//  }
package kafka

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
)

type Consumer struct {
	cfg      *Config
	consumer sarama.Consumer
	broker   *sarama.Broker
	dataChan chan *sarama.ConsumerMessage
	errChan  chan error

	waitGroup sync.WaitGroup
	stopChan  chan struct{}

	offsets     map[int32]int64
	offsetGuard sync.Mutex

	closed int32
}

type Logger interface {
	Error(args ...interface{})
	Notice(args ...interface{})
}

type Config struct {
	Kafka        []string
	Topic        string
	Partition    []int32
	GroupName    string
	OldestOffset bool
	Logger       Logger
}

func (c *Config) Validate() error {
	if len(c.Kafka) == 0 {
		return fmt.Errorf("kafka address is required")
	}
	if c.GroupName == "" {
		return fmt.Errorf("groupName is required")
	}
	if c.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	return nil
}

func NewConsumer(cfg *Config) (*Consumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	consumer, err := sarama.NewConsumer(cfg.Kafka, nil)
	if err != nil {
		return nil, err
	}

	c := &Consumer{
		cfg:      cfg,
		consumer: consumer,
		offsets:  make(map[int32]int64),
		dataChan: make(chan *sarama.ConsumerMessage),
		errChan:  make(chan error),

		stopChan: make(chan struct{}),
	}

	c.broker, err = c.getBroker()
	if err != nil {
		return nil, err
	}

	go c.start()
	return c, nil
}

func (c *Consumer) Commit(msg *sarama.ConsumerMessage) {
	c.commitOffset(msg.Partition, msg.Offset)
}

func (c *Consumer) commitOffset(partition int32, offset int64) {
	c.offsetGuard.Lock()
	c.offsets[partition] = offset
	c.offsetGuard.Unlock()
}

func (c *Consumer) FlushOffset() error {
	req := &sarama.OffsetCommitRequest{
		ConsumerGroup: c.cfg.GroupName,
	}

	hasBlock := false
	c.offsetGuard.Lock()
	for k, v := range c.offsets {
		hasBlock = true
		req.AddBlock(c.cfg.Topic, k, v, 0, "")
	}
	c.offsetGuard.Unlock()

	if !hasBlock {
		return nil
	}
	_, err := c.broker.CommitOffset(req)
	return err
}

func (c *Consumer) getBroker() (*sarama.Broker, error) {
	broker := sarama.NewBroker(c.cfg.Kafka[0])
	err := broker.Open(nil)
	if err != nil {
		return nil, err
	}
	resp, err := broker.GetConsumerMetadata(&sarama.ConsumerMetadataRequest{
		ConsumerGroup: c.cfg.GroupName,
	})

	if err != nil {
		return nil, err
	}
	if err := resp.Coordinator.Open(nil); err != nil {
		return nil, err
	}

	return resp.Coordinator, nil
}

func (c *Consumer) getOffsets(topic string, partitions []int32) ([]int64, error) {
	req := &sarama.OffsetFetchRequest{
		ConsumerGroup: c.cfg.GroupName,
	}
	for _, partition := range partitions {
		req.AddPartition(topic, partition)
	}

	offsets := make([]int64, len(partitions))
	resp, err := c.broker.FetchOffset(req)
	if err != nil {
		return nil, fmt.Errorf("fetchOffset: %v", err)
	}
	for idx, partition := range partitions {
		blk := resp.GetBlock(topic, partition)
		if blk.Err == sarama.ErrNoError {
			offsets[idx] = blk.Offset
		} else if blk.Err == sarama.ErrUnknownTopicOrPartition {
			if c.cfg.OldestOffset {
				offsets[idx] = sarama.OffsetOldest
			} else {
				offsets[idx] = blk.Offset
			}
		} else {
			return nil, blk.Err
		}

	}

	return offsets, nil
}

func (c *Consumer) consumePartition(errChan chan error, msgChan chan *sarama.ConsumerMessage, topic string, partition int32, offset int64) {
	c.waitGroup.Add(1)
	defer c.waitGroup.Done()

	cp, err := c.consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		errChan <- fmt.Errorf("consume partition error: [t:%v/p:%v] %v", topic, partition, err)
		return
	}
	defer cp.Close()

loop:
	for {
		select {
		case msg := <-cp.Messages():
			select {
			case msgChan <- msg:
			case <-c.stopChan:
				break loop
			}
		case <-c.stopChan:
			break loop
		}
	}
}

func (c *Consumer) GetPartitions() ([]int32, error) {
	return c.consumer.Partitions(c.cfg.Topic)
}

func (c *Consumer) Message() chan *sarama.ConsumerMessage {
	return c.dataChan
}

func (c *Consumer) Error() chan error {
	return c.errChan
}

func (c *Consumer) flushOffsetLoop() {
	c.waitGroup.Add(1)
	defer c.waitGroup.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ticker.C:
			err := c.FlushOffset()
			if err != nil {
				if c.cfg.Logger != nil {
					c.cfg.Logger.Error("flush offset error: ", err)
				}
			}
		case <-c.stopChan:
			break loop
		}
	}
}

// Start will blocking until some error is occurred or call `Close` manually
func (c *Consumer) start() {
	var err error
	if len(c.cfg.Partition) == 0 {
		c.cfg.Partition, err = c.GetPartitions()
		if err != nil {
			c.errChan <- fmt.Errorf("error in get partitions: %v", err)
			return
		}
	}

	offsets, err := c.getOffsets(c.cfg.Topic, c.cfg.Partition)
	if err != nil {
		c.errChan <- fmt.Errorf("error in fetch offset: %v", err)
		return
	}
	for idx, p := range c.cfg.Partition {
		go c.consumePartition(c.errChan, c.dataChan, c.cfg.Topic, p, offsets[idx])
	}

	// blocking
	c.flushOffsetLoop()
}

func (c *Consumer) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		// TODO: force close?
		return nil
	}
	close(c.stopChan)
	close(c.dataChan)
	close(c.errChan)
	for _ = range c.dataChan {
	}
	c.waitGroup.Wait()

	if err := c.FlushOffset(); err != nil {
		// flush to disk?
		return err
	}

	return nil
}
