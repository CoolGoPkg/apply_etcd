package nsqd

import (
	"fmt"

	"github.com/nsqio/go-nsq"
)

//  CreateNSQConsumer 新建消费者
func CreateNSQConsumer(topicName, channelName string, lookupAddressList []string,
	messageHandler nsq.Handler) *nsq.Consumer {
	configObj := nsq.NewConfig()
	consumerInstance, err := nsq.NewConsumer(topicName, channelName, configObj)
	if err != nil {
		content := fmt.Sprintf("CreateNSQConsumer NewConsumer err:%s topic:%s channel:%s",
			err, topicName, channelName)
		panic(content)
	}
	consumerInstance.AddHandler(messageHandler)
	consumerInstance.ChangeMaxInFlight(1000)
	if err := consumerInstance.ConnectToNSQLookupds(lookupAddressList); err != nil {
		content := fmt.Sprintf("CreateNSQConsumer ConnectToNSQLookupds err:%s topic:%s channel:%s",
			err, topicName, channelName)
		panic(content)
	}
	return consumerInstance
}
