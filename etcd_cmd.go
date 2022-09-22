package main

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

type Client struct {
	Cli *clientv3.Client
}

func NewClient() (*Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("etcd client err : ", err)
		return nil, err
		// handle error!
	}

	return &Client{
		cli,
	}, nil
}

func (c *Client) Put(key, value string) {
	defer c.Cli.Close()
	res, err := c.Cli.KV.Put(context.Background(), key, value)
	if err != nil {
		return
	}
	fmt.Println("res : ", res)
}

func (c *Client) Get(key string) {
	defer c.Cli.Close()
	resp, err := c.Cli.KV.Get(context.Background(), key)
	if err != nil {
		return
	}
	fmt.Println("resp : ", resp)
}
