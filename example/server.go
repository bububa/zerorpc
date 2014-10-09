package main

import (
	"github.com/bububa/zerorpc"
	"log"
	//"time"
)

func main() {
	server, err := zerorpc.NewServer("tcp://*:5000", 10)
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()
	hello := func(v []interface{}) (interface{}, error) {
		return "Hello, " + v[0].(string), nil
	}
	server.RegisterTask("hello", &hello)
	server.Run()
}
