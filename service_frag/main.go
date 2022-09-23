package main

import (
	"CoolGoPkg/apply_etcd/service_frag/conf"
	"fmt"
	"time"
)

func main() {
	defer func() {
		time.Sleep(2 * time.Second)
		fmt.Println("clean and eligible Quit...")
	}()

	conf.InitConfig("/Users/mac/go/src/CoolGoPkg/apply_etcd/service_frag/conf/conf.yaml")
	fmt.Println(conf.Config)

	srv, err := NewService(conf.Config.Registry, conf.Config.QuorumCap, false)
	if err != nil {
		panic(err)
	}

	err = srv.Start()
	if err != nil {
		fmt.Println("service start err :", err)
	}

}
