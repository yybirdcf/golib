package queue

import (
	"os"

	"github.com/Shopify/sarama"
	"github.com/yybirdcf/golib/clog"
)

type KafkaConfig struct {
	Addresses []string
}

type KafkaQueue struct {
	producer sarama.SyncProducer
	consumer sarama.Consumer
	handlers map[string]func(interface{})
}

func NewKafkaQueue(cfg *KafkaConfig) *KafkaQueue {
	config := sarama.NewConfig()
	// 等待服务器所有副本都保存成功后的响应
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 随机的分区类型：返回一个分区器，该分区器每次选择一个随机分区
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	// 是否等待成功和失败后的响应
	config.Producer.Return.Successes = true
	// 使用给定代理地址和配置创建一个同步生产者
	producer, err := sarama.NewSyncProducer(cfg.Addresses, config)
	if err != nil {
		clog.Errorf("instance kafka producer err: %v", err)
		os.Exit(-1)
	}

	// 根据给定的代理地址和配置创建一个消费者
	consumer, err := sarama.NewConsumer(cfg.Addresses, nil)
	if err != nil {
		clog.Errorf("instance kafka producer err: %v", err)
		os.Exit(-1)
	}

	kq := &KafkaQueue{}
	kq.producer = producer
	kq.consumer = consumer
	kq.handlers = make(map[string]func(interface{}))

	return kq
}

func (kq *KafkaQueue) Push(name string, value string) error {
	//构建发送的消息
	msg := &sarama.ProducerMessage{
		Topic: name, //包含了消息的主题
	}

	msg.Value = sarama.ByteEncoder(value)
	//SendMessage：该方法是生产者生产给定的消息
	//生产成功的时候返回该消息的分区和所在的偏移量
	//生产失败的时候返回error
	partition, offset, err := kq.producer.SendMessage(msg)

	if err != nil {
		clog.Errorf("Send message Fail: %+v", msg)
	}
	clog.Info("Partition = %d, offset=%d\n", partition, offset)
	return err
}

func (kq *KafkaQueue) Close() {
	if kq.producer != nil {
		kq.producer.Close()
	}

	if kq.consumer != nil {
		kq.consumer.Close()
	}
}

func (kq *KafkaQueue) RegisterHandler(name string, handler func(interface{})) {
	kq.handlers[name] = handler
}

func (kq *KafkaQueue) Run() {
	for name, _ := range kq.handlers {
		kq.runHandler(name)
	}
}

func (kq *KafkaQueue) runHandler(name string) {
	//Partitions(topic):该方法返回了该topic的所有分区id
	partitionList, err := kq.consumer.Partitions(name)
	if err != nil {
		clog.Errorf("get partition list: %v", err)
		return
	}

	for partition := range partitionList {
		//ConsumePartition方法根据主题，分区和给定的偏移量创建创建了相应的分区消费者
		//如果该分区消费者已经消费了该信息将会返回error
		//sarama.OffsetNewest:表明了为最新消息
		pc, err := kq.consumer.ConsumePartition(name, int32(partition), sarama.OffsetNewest)
		if err != nil {
			clog.Errorf("consume partition: %v", err)
			return
		}
		defer pc.AsyncClose()

		go func(sarama.PartitionConsumer) {
			//Messages()该方法返回一个消费消息类型的只读通道，由代理产生
			for msg := range pc.Messages() {
				kq.handlers[name](msg.Value)
			}
		}(pc)
	}
}
