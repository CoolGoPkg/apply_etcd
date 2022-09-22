package nsqd

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"
)

type Data struct {
	ID string
}

func TestCreateProducer(t *testing.T) {
	nsqAddr := "127.0.0.1:4150"
	producer := CreateProducer(nsqAddr)
	fmt.Println("create success")
	for i := 0; i < 10000; i++ {
		fmt.Println(i)
		testData := Data{
			ID: "测试消息：" + strconv.Itoa(i),
		}
		sendData, err := json.Marshal(testData)
		if err != nil {
			t.Log("marshal err : ", err)
			return
		}
		err = producer.Publish("master-data-topic-1", sendData)
		if err != nil {
			fmt.Println(err)
			t.Log("pubulish err : ", err)
			continue
		}
		time.Sleep(time.Second)
	}

}
