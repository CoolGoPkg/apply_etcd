package main

import "testing"

func TestClient_Put(t *testing.T) {
	cli, err := NewClient()
	if err != nil {
		t.Log("err : ", err)
		return
	}
	cli.Put("aaa", "bbb")
}

func TestClient_Get(t *testing.T) {
	cli, err := NewClient()
	if err != nil {
		t.Log("err : ", err)
		return
	}
	cli.Get("aaa")
}
