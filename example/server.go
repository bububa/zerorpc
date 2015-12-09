package main

import (
	"github.com/bububa/zerorpc"
	"log"
	"time"
)

type Tmp struct {
	W string
	T time.Time
	N []uint64
}

func main() {
	server, err := zerorpc.NewServer("tcp://*:5000", "router1:2181,code1:2181,code2:2181,code3:2181,code4:2181,code5:2181,code6:2181", "/services/rpc/dsp", 10)
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()
	hello := func(v []interface{}) (interface{}, error) {
		return &Tmp{W: "Hello, " + v[0].(string), T: time.Now(), N: []uint64{77223396834, 77223396833, 77223396835}}, nil
	}
	server.RegisterTask("hello", &hello)
	err = server.Run()
	if err != nil {
		log.Fatal(err)
	}
}
