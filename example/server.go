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
	server, err := zerorpc.NewServer("tcp://*:5000", 10)
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()
	hello := func(v []interface{}) (interface{}, error) {
		return &Tmp{W: "Hello, " + v[0].(string), T: time.Now(), N: []uint64{77223396834, 77223396833, 77223396835}}, nil
	}
	server.RegisterTask("hello", &hello)
	server.Run()
}
