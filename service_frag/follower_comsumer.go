package main

import (
	"CoolGoPkg/apply_etcd/service_frag/conf"
	"CoolGoPkg/apply_etcd/service_frag/nsqd"
	"CoolGoPkg/apply_etcd/service_frag/util"
	"encoding/json"
	"fmt"
	"github.com/nsqio/go-nsq"
)

type FollowerConsumer struct {
	topic    string
	consumer *nsq.Consumer
}

func InitFollowerConsumer(index int, config conf.ConfigNSQ) *FollowerConsumer {
	consumer := new(FollowerConsumer)
	consumer.topic = fmt.Sprintf(followerTopic, index)
	consumer.consumer = nsqd.CreateNSQConsumer(consumer.topic, channel, config.LookupAddress, consumer)
	fmt.Println("start consume real::::", consumer.topic)
	return consumer
}

func (self *FollowerConsumer) HandleMessage(message *nsq.Message) error {
	var testData = new(Data)
	if err := json.Unmarshal(message.Body, testData); err != nil {
		fmt.Printf("HandleMessage, failed to unmarshal tick data: %s, err: %v \n", string(message.Body), err)
		return err
	}
	fmt.Printf("follower consumer message : %s   topic index : %d \n", string(message.Body), util.GetIndex(testData.ID, conf.Config.QuorumCap-1, 1))

	return nil
}

func (self *FollowerConsumer) Stop() {
	self.consumer.Stop()
	fmt.Println("stop consume real::::", self.topic)
}
