package main

import (
	"CoolGoPkg/apply_etcd/service_frag/conf"
	"CoolGoPkg/apply_etcd/service_frag/nsqd"
	"CoolGoPkg/apply_etcd/service_frag/util"
	"context"
	"encoding/json"
	"fmt"
	"github.com/nsqio/go-nsq"
)

const (
	followerTopic = "follower.queue.test.%d.topic"
	channel       = "channel.queue.test"
)

type LeaderConsumers struct {
	consumers []*LeaderConsumer
	cancel    context.CancelFunc
}

type LeaderConsumer struct {
	topic     string
	QurumCap  int
	consumer  *nsq.Consumer
	sche      map[int]*LeaderSched
	hashIndex map[string]int
}

type LeaderSched struct {
	index    int
	Channel  chan []byte
	Ctx      context.Context
	Producer *nsq.Producer
	Topic    string
}

func InitLeaderDistribute(nsqConf conf.ConfigNSQ, channel chan []byte, ctx context.Context, index int) *LeaderSched {
	sched := &LeaderSched{
		index:    index,
		Channel:  channel,
		Ctx:      ctx,
		Producer: nsqd.CreateProducer(nsqConf.NsqdAddress),
		Topic:    fmt.Sprintf(followerTopic, index),
	}
	sched.Publish()
	return sched
}

func (self *LeaderSched) Publish() {
	go func() {
		for {
			select {
			case v, ok := <-self.Channel:
				if !ok {
					return
				}
				self.Producer.PublishAsync(self.Topic, v, nil)
			case <-self.Ctx.Done():
				return
			}
		}
	}()
}

func InitLeaderConsumer(topics []string, config conf.ConfigNSQ) *LeaderConsumers {
	consumers := new(LeaderConsumers)
	souces := make([]*LeaderConsumer, 0, len(topics))
	var cap int
	if ServiceInstance.quorum.IsMasterConsume {
		cap = ServiceInstance.quorum.QuorumCap
	} else {
		cap = ServiceInstance.quorum.QuorumCap - 1
	}
	sche := make(map[int]*LeaderSched)
	ctx, cancel := context.WithCancel(context.Background())
	consumers.cancel = cancel
	for i := 0; i < cap; i++ {
		channel := make(chan []byte, 10000)
		sched := InitLeaderDistribute(config, channel, ctx, i)
		sche[i] = sched
	}
	for _, topic := range topics {
		consumer := new(LeaderConsumer)
		consumer.topic = topic
		consumer.sche = sche
		consumer.QurumCap = cap
		consumer.hashIndex = make(map[string]int)
		consumer.consumer = nsqd.CreateNSQConsumer(topic, channel, config.LookupAddress, consumer)
		souces = append(souces, consumer)
	}
	consumers.consumers = souces
	return consumers
}

type Data struct {
	ID string
}

func (self *LeaderConsumer) HandleMessage(message *nsq.Message) error {

	fmt.Println("收到消息： ", string(message.Body))
	var testData = new(Data)
	if err := json.Unmarshal(message.Body, testData); err != nil {
		fmt.Printf("HandleMessage, failed to unmarshal tick data: %s, err: %v", string(message.Body), err)
		return err
	}

	index := self.GetIndex(testData.ID)
	fmt.Printf("发送消息 ： %s 到 %d \n", testData.ID, index)
	self.sche[index].Channel <- message.Body

	return nil
}

func (self *LeaderConsumer) GetIndex(code string) int {
	if v, ok := self.hashIndex[code]; ok {
		return v
	} else {
		index := util.GetIndex(code, self.QurumCap, 1)
		self.hashIndex[code] = index
		return index
	}
}

func (self *LeaderConsumers) Stop() {
	for _, c := range self.consumers {
		c.consumer.Stop()
		<-c.consumer.StopChan
		fmt.Println("leader consumer consume topic exit:", c.topic)
	}
	self.cancel()
}
