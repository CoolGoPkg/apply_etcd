package nsqd

import (
	"fmt"
	"github.com/nsqio/go-nsq"
)

func CreateProducer(nsqdAddress string) *nsq.Producer {
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(nsqdAddress, config)
	if err != nil {
		fmt.Println("FBI WARNING BUG BUG CreateProducer NewProducer Error...", nsqdAddress, err, producer)
	} else {
		producer.Ping()
	}
	return producer
}
