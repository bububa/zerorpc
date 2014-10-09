package main

import (
	"fmt"
	"github.com/bububa/zerorpc"
	"sync"
)

func main() {
	c, err := zerorpc.NewClient("tcp://0.0.0.0:5000")
	if err != nil {
		panic(err)
	}

	defer c.Close()
	var wg sync.WaitGroup
	for i := 0; i <= 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := c.InvokeAsync("hello", "John")
			if err != nil {
				fmt.Println(err)
			}

			fmt.Println(response)
		}()
	}
	wg.Wait()
	response, err := c.Invoke("hello", "Syd")
	fmt.Println(response)
	c.Close()
}
