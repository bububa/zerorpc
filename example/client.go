package main

import (
	"fmt"
	"github.com/XiBao/taobao/utils"
	"github.com/bububa/zerorpc"
	"reflect"
	"sync"
	"time"
)

var (
	timeTyp = reflect.TypeOf(time.Time{})

	timeLoc = time.Now().Location()

	timeEncExt = func(rv reflect.Value) ([]byte, error) {
		return []byte(rv.Interface().(time.Time).Format(time.RFC3339)), nil
	}

	timeDecExt = func(rv reflect.Value, bs []byte) error {
		tt := utils.ParseTime(string(bs), timeLoc)
		rv.Set(reflect.ValueOf(tt))
		return nil
	}
)

type Tmp struct {
	W string
	T time.Time
	N []uint64
}

func main() {
	c, err := zerorpc.NewClient("tcp://0.0.0.0:5000")
	if err != nil {
		panic(err)
	}
	defer c.Close()
	c.ConnectPool(6)
	var wg sync.WaitGroup
	for i := 0; i <= 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := c.Invoke("hello", "John")
			if err != nil {
				fmt.Println(err)
			}

			fmt.Println(response)
		}()
	}
	wg.Wait()
	c.PoolDisconnect()
	response, err := c.Invoke("hello", "Syd")
	fmt.Println(response)
}
