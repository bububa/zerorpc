package main

import (
	"encoding/json"
	"fmt"
	"github.com/XiBao/taobao/utils"
	"github.com/bububa/go/codec"
	"github.com/bububa/zerorpc"
	"reflect"
	//"sync"
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
	/*var wg sync.WaitGroup
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
	wg.Wait()*/
	response, err := c.Invoke("hello", "Syd")
	fmt.Println(response)
	args := response.Args
	fmt.Println(args[0].(map[interface{}]interface{})["N"].([]interface{}))
	var (
		mh      codec.MsgpackHandle
		buf     []byte
		request Tmp
	)
	mh.AddExt(timeTyp, 1, timeEncExt, timeDecExt)
	enc := codec.NewEncoderBytes(&buf, &mh)
	if err := enc.Encode(args[0]); err != nil {
		fmt.Println(err)
		return
	}
	dec := codec.NewDecoderBytes(buf, &mh)
	err = dec.Decode(&request)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(request)
	c.Close()
}
