package main

import (
	"github.com/bububa/zerorpc"
	"log"
	"time"
)

type Tmp struct {
	W string
	T time.Time
}

func main() {
	server, err := zerorpc.NewServer("tcp://*:5000", 10)
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()
	hello := func(v []interface{}) (interface{}, error) {
		return &Tmp{W: "Hello, " + v[0].(string), T: time.Now()}, nil
	}
	server.RegisterTask("hello", &hello)
	server.Run()
}
