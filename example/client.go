package main

import (
	"fmt"
	"github.com/XiBao/common/utils"
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
	c, err := zerorpc.NewClient("router1:2181,code1:2181,code2:2181,code3:2181,code4:2181,code5:2181,code6:2181", "/services/rpc/dsp")
	if err != nil {
		panic(err)
	}
	defer c.Close()
	c.Connect()
	var wg sync.WaitGroup
	for i := 0; i <= 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.Invoke("hello", "John")
			if err != nil {
				fmt.Println(err)
			}

			//fmt.Println(response)
		}()
		//time.Sleep(20 * time.Second)
	}
	wg.Wait()
	_, err = c.Invoke("hello", "Syd")
	//fmt.Println(response)
}
