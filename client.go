package zerorpc

import (
	"fmt"
	"github.com/kisielk/raven-go/raven"
	uuid "github.com/nu7hatch/gouuid"
	zmq "github.com/pebbe/zmq4"
	"math/rand"
	"runtime/debug"
)

const SENTRY_DNS = "https://40a8b3a4b5724b73a182a45dd888583e:082932b71c7a489cb32a9813acbb33b4@sentry.xibao100.com/3"

// ZeroRPC client representation,
// it holds a pointer to the ZeroMQ socket
type Client struct {
	endpoint        string
	context         *zmq.Context
	dealerPool      []*zmq.Socket
	routerPool      []*zmq.Socket
	routerEndpoints []string
}

// Connects to a ZeroRPC endpoint and returns a pointer to the new client
func NewClient(endpoint string) (*Client, error) {
	context, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}

	c := &Client{
		endpoint: endpoint,
		context:  context,
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

func (c *Client) invoke(ev *Event) (*Event, error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			sentry, _ := raven.NewClient(SENTRY_DNS)
			if sentry != nil {
				sentry.CaptureMessage(string(debug.Stack()))
			}
		}
	}()
	var endpoint string
	if c.routerEndpoints == nil || len(c.routerEndpoints) == 0 {
		endpoint = c.endpoint
	} else {
		endpoint = c.randRouterEndpoint()
	}
	workerSocket, err := c.context.NewSocket(zmq.REQ)
	if err != nil {
		return nil, err
	}
	defer workerSocket.Close()
	if err := workerSocket.Connect(endpoint); err != nil {
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

func (c *Client) Invoke(name string, args ...interface{}) (*Event, error) {
	ev, err := newEvent(name, args)
	if err != nil {
		return nil, err
	}
	return c.invoke(ev)
}

func (c *Client) InvokeBlackHole(name string, args ...interface{}) (*Event, error) {
	ev, err := newEvent(name, args)
	if err != nil {
		return nil, err
	}
	ev.toBlackHole()
	return c.invoke(ev)
}

func (c *Client) ConnectPool(poolSize int) error {
	var n int
	for n < poolSize {
		dealerSocket, err := c.context.NewSocket(zmq.DEALER)
		if err != nil {
			continue
		}
		if err := dealerSocket.Connect(c.endpoint); err != nil {
			continue
		}

		uid, err := uuid.NewV4()
		if err != nil {
			dealerSocket.Close()
			continue
		}

		routerEndpoint := fmt.Sprintf("inproc://%s", uid)

		routerSocket, err := c.context.NewSocket(zmq.ROUTER)
		if err != nil {
			dealerSocket.Close()
			continue
		}
		if err := routerSocket.Bind(routerEndpoint); err != nil {
			dealerSocket.Close()
			continue
		}
		c.dealerPool = append(c.dealerPool, dealerSocket)
		c.routerPool = append(c.routerPool, routerSocket)
		c.routerEndpoints = append(c.routerEndpoints, routerEndpoint)
		go zmq.Proxy(dealerSocket, routerSocket, nil)
		n += 1
	}
	return nil
}

func (c *Client) DisconnectPool() {
	c.routerEndpoints = []string{}
	if c.dealerPool != nil {
		for _, dealerSocket := range c.dealerPool {
			if dealerSocket != nil {
				dealerSocket.Close()
			}
		}
	}
	c.dealerPool = []*zmq.Socket{}

	if c.routerPool != nil {
		for _, routerSocket := range c.routerPool {
			if routerSocket != nil {
				routerSocket.Close()
			}
		}
	}
	c.routerPool = []*zmq.Socket{}

}

// Closes the ZeroMQ socket
func (c *Client) Close() {
	c.DisconnectPool()
	c.context.Term()
}

func randNum(from int, to int) int {
	if from == to {
		return to
	}
	if from > to {
		from, to = to, from
	}
	return rand.Intn(to-from) + from
}

func (c *Client) randRouterEndpoint() string {
	return c.routerEndpoints[randNum(0, len(c.routerEndpoints))]
}
