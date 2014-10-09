package zerorpc

import (
	"fmt"
	uuid "github.com/bububa/gouuid"
	zmq "github.com/bububa/zmq4"
)

// ZeroRPC client representation,
// it holds a pointer to the ZeroMQ socket
type Client struct {
	endpoint     string
	context      *zmq.Context
	dealerSocket *zmq.Socket
}

// Connects to a ZeroRPC endpoint and returns a pointer to the new client
func NewClient(endpoint string) (*Client, error) {
	context, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}

	dealerSocket, err := context.NewSocket(zmq.DEALER)
	if err != nil {
		return nil, err
	}
	if err := dealerSocket.Connect(endpoint); err != nil {
		return nil, err
	}

	c := &Client{
		endpoint:     endpoint,
		context:      context,
		dealerSocket: dealerSocket,
	}

	return c, nil
}

/*
Invokes a ZeroRPC method,
name is the method name,
args are the method arguments

it returns the ZeroRPC response event on success

if the ZeroRPC server raised an exception,
it's name is returned as the err string along with the response event,
the additional exception text and traceback can be found in the response event args

it returns ErrLostRemote if the channel misses 2 heartbeat events,
default is 10 seconds

Usage example:

    package main

    import (
        "fmt"
        "github.com/bububa/zerorpc"
    )

    func main() {
        c, err := zerorpc.NewClient("tcp://0.0.0.0:4242")
        if err != nil {
            panic(err)
        }

        defer c.Close()

        response, err := c.Invoke("hello", "John")
        if err != nil {
            panic(err)
        }

        fmt.Println(response)
    }

It also supports first class exceptions, in case of an exception,
the error returned from Invoke() or InvokeStream() is the exception name
and the args of the returned event are the exception description and traceback.

The client sends heartbeat events every 5 seconds, if twp heartbeat events are missed,
the remote is considered as lost and an ErrLostRemote is returned.
*/

func (c *Client) Invoke(name string, args ...interface{}) (*Event, error) {
	ev, err := newEvent(name, args)
	if err != nil {
		return nil, err
	}
	workerSocket, err := c.context.NewSocket(zmq.REQ)
	if err != nil {
		return nil, err
	}
	defer workerSocket.Close()
	if err := workerSocket.Connect(c.endpoint); err != nil {
		return nil, err
	}
	responseBytes, err := ev.packBytes()
	if err != nil {
		return nil, err
	}
	workerSocket.SendMessage("", responseBytes)
	var responseEvent *Event
	for {
		barr, err := workerSocket.RecvMessageBytes(0)
		responseEvent, err = unPackBytes(barr[len(barr)-1])
		return responseEvent, err
	}
	return nil, nil
}

func (c *Client) InvokeAsync(name string, args ...interface{}) (*Event, error) {
	ev, err := newEvent(name, args)
	if err != nil {
		return nil, err
	}
	uid, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	dealerSocket, err := c.context.NewSocket(zmq.DEALER)
	if err != nil {
		return nil, err
	}
	defer dealerSocket.Close()
	if err := dealerSocket.Connect(c.endpoint); err != nil {
		return nil, err
	}

	routerEndpoint := fmt.Sprintf("inproc://%s", uid)
	routerSocket, err := c.context.NewSocket(zmq.ROUTER)
	if err != nil {
		return nil, err
	}
	defer routerSocket.Close()
	if err := routerSocket.Bind(routerEndpoint); err != nil {
		return nil, err
	}

	go zmq.Proxy(dealerSocket, routerSocket, nil)

	workerSocket, err := c.context.NewSocket(zmq.REQ)
	if err != nil {
		return nil, err
	}
	defer workerSocket.Close()
	if err := workerSocket.Connect(routerEndpoint); err != nil {
		return nil, err
	}

	responseBytes, err := ev.packBytes()
	if err != nil {
		return nil, err
	}
	workerSocket.SendMessage("", responseBytes)
	var responseEvent *Event
	for {
		barr, err := workerSocket.RecvMessageBytes(0)
		responseEvent, err = unPackBytes(barr[len(barr)-1])
		return responseEvent, err
	}
	return nil, nil
}

// Closes the ZeroMQ socket
func (c *Client) Close() {
	c.dealerSocket.Close()
	c.context.Term()
}
