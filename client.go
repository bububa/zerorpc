package zerorpc

import (
	"errors"
	"fmt"
	"github.com/getsentry/raven-go"
	zmq "github.com/pebbe/zmq4"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"time"
)

const (
	MAX_RETRIES = 5
)

// ZeroRPC client representation,
// it holds a pointer to the ZeroMQ socket
type Client struct {
	endpoint string
	context  *zmq.Context
	cluster  *Cluster
	sentry   *raven.Client
}

// Connects to a ZeroRPC endpoint and returns a pointer to the new client
func NewClient(zkHosts []string, zkPath string) (*Client, error) {
	zkConn, _, err := zk.Connect(zkHosts, time.Second*10)
	if err != nil {
		return nil, err
	}
	context, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}

	c := &Client{
		context: context,
		cluster: NewCluster(zkConn, zkPath),
	}

	return c, nil
}

func NewSingleClient(endpoint string) (*Client, error) {
	context, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}

	c := &Client{
		context:  context,
		endpoint: endpoint,
	}

	return c, nil
}

func (this *Client) SetSentry(sentry *raven.Client) {
	this.sentry = sentry
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
		if c.sentry != nil {
			var packet *raven.Packet
			switch rval := recover().(type) {
			case nil:
				return
			case error:
				packet = raven.NewPacket(rval.Error(), raven.NewException(rval, raven.NewStacktrace(0, 3, nil)))
			default:
				rvalStr := fmt.Sprint(rval)
				packet = raven.NewPacket(rvalStr, raven.NewException(errors.New(rvalStr), raven.NewStacktrace(0, 3, nil)))
			}
			_, ch := c.sentry.Capture(packet, nil)
			if errSentry := <-ch; errSentry != nil {
				log.Println(errSentry)
			}
		} else if recovered := recover(); recovered != nil {
			log.Println(recovered)
		}
	}()
	workerSocket, err := c.context.NewSocket(zmq.REQ)
	if err != nil {
		return nil, err
	}
	defer workerSocket.Close()
	var (
		connectionErr error
		retry         int
	)
	for retry < MAX_RETRIES {
		var endpoint string
		if c.endpoint != "" {
			endpoint = c.endpoint
		} else {
			node := c.cluster.GetNode()
			if node == "" {
				connectionErr = errors.New("No Available RPC Node")
				retry += 1
				continue
			}
			endpoint = fmt.Sprintf("tcp://%s", node)
		}
		if connectionErr = workerSocket.Connect(endpoint); connectionErr != nil {
			retry += 1
			continue
		} else {
			connectionErr = nil
			break
		}
	}
	if connectionErr != nil {
		return nil, connectionErr
	}
	responseBytes, err := ev.packBytes()
	if err != nil {
		return nil, err
	}
	workerSocket.SendMessage("", responseBytes)
	var responseEvent *Event
	for {
		barr, err := workerSocket.RecvMessageBytes(0)
		if err != nil {
			return nil, err
		}
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

func (c *Client) Connect() {
	c.cluster.Connect()
}

// Closes the ZeroMQ socket
func (c *Client) Close() {
	if c.cluster != nil {
		c.cluster.Close()
	}
	c.context.Term()
}
