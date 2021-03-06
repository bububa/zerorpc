package zerorpc

import (
	"errors"
	"fmt"
	"github.com/getsentry/raven-go"
	log "github.com/kdar/factorlog"
	zmq "github.com/pebbe/zmq4"
	"github.com/samuel/go-zookeeper/zk"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ZeroRPC server representation,
// it holds a pointer to the ZeroMQ socket
type Server struct {
	context      *zmq.Context
	routerSocket *zmq.Socket
	dealerSocket *zmq.Socket
	zkPath       string
	hostname     string
	zkConn       *zk.Conn
	maxWorkers   int
	logger       *log.FactorLog
	sentry       *raven.Client
	wg           sync.WaitGroup
	handlers     map[string]*func(v []interface{}) (interface{}, error)
}

const (
	DEALER_ENDPOINT     = "inproc://workers"
	DEFAULT_MAX_WORKERS = 1024
)

var (
	ErrDuplicateHandler = errors.New("zerorpc/server duplicate task handler")
	ErrNoTaskHandler    = errors.New("zerorpc/server no handler for task")
)

/*
Binds to a ZeroRPC endpoint and returns a pointer to the new server

Usage example:

    package main

    import (
        "errors"
        "fmt"
        "github.com/bububa/zerorpc"
        "time"
    )

    func main() {
        s, err := zerorpc.NewServer("tcp://0.0.0.0:4242")
        if err != nil {
            panic(err)
        }

        defer s.Close()

        h := func(v []interface{}) (interface{}, error) {
            time.Sleep(10 * time.Second)
            return "Hello, " + v[0].(string), nil
        }

        s.RegisterTask("hello", &h)

        s.Listen()
    }

It also supports first class exceptions, in case of the handler function returns an error,
the args of the event passed to the client is an array which is [err.Error(), nil, nil]
*/

func NewServer(endpoint string, zkHosts []string, zooPath string, maxWorkers int) (*Server, error) {
	zoo, _, err := zk.Connect(zkHosts, time.Second*10)
	if err != nil {
		return nil, err
	}
	_, err = CreateRecursive(zoo, zooPath, "", 0, DefaultDirACLs())
	if err != nil {
		return nil, err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	tmpArr := strings.Split(endpoint, ":")
	host := fmt.Sprintf("%s:%s", hostname, tmpArr[len(tmpArr)-1])
	context, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}
	routerSocket, err := context.NewSocket(zmq.ROUTER)
	if err != nil {
		return nil, err
	}
	if err := routerSocket.Bind(endpoint); err != nil {
		return nil, err
	}

	dealerSocket, err := context.NewSocket(zmq.DEALER)
	if err != nil {
		return nil, err
	}
	if err := dealerSocket.Bind(DEALER_ENDPOINT); err != nil {
		return nil, err
	}

	if maxWorkers <= 0 {
		maxWorkers = DEFAULT_MAX_WORKERS
	}

	server := &Server{
		context:      context,
		routerSocket: routerSocket,
		dealerSocket: dealerSocket,
		hostname:     host,
		zkPath:       zooPath,
		zkConn:       zoo,
		maxWorkers:   maxWorkers,
		handlers:     make(map[string]*func(v []interface{}) (interface{}, error)),
	}

	return server, nil
}

func (s *Server) SetSentry(sentry *raven.Client) {
	s.sentry = sentry
}

// SetLogger 初始化设置logger
func (s *Server) SetLogger(alogger *log.FactorLog) {
	s.logger = alogger
}

func (s *Server) createZkNode() error {
	zkTicker := time.NewTicker(time.Second * 1)
	for {
		select {
		case <-zkTicker.C:
			_, err := s.zkConn.Create(path.Join(s.zkPath, s.hostname), nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
			if ErrorEqual(err, zk.ErrNodeExists) {
				continue
			} else if err != nil {
				return err
			} else {
				return nil
			}
		}
	}
	return nil
}

func (s *Server) Run() error {
	err := s.createZkNode()
	if err != nil {
		return err
	}
	for i := 0; i < s.maxWorkers; i++ {
		go s.listen()
	}
	zmq.Proxy(s.routerSocket, s.dealerSocket, nil)
	return nil
}

// Closes the ZeroMQ socket
func (s *Server) Close() {
	err := s.zkConn.Delete(path.Join(s.zkPath, s.hostname), 0)
	if err != nil {
		s.logger.Error(err)
	}
	s.zkConn.Close()
	s.routerSocket.Close()
	s.dealerSocket.Close()
	//s.context.Term()
	s.wg.Wait()
}

// Register a task handler,
// tasks are invoked in new goroutines
//
// it returns ErrDuplicateHandler if an handler was already registered for the task
func (s *Server) RegisterTask(name string, handlerFunc *func(v []interface{}) (interface{}, error)) error {
	if _, found := s.handlers[name]; found {
		return ErrDuplicateHandler
	}
	s.handlers[name] = handlerFunc
	return nil
}

// Invoke the handler for a task event,
// it returns ErrNoTaskHandler if no handler is registered for the task
func (s *Server) handleTask(ev *Event) (interface{}, error) {
	defer func() {
		if s.sentry != nil {
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
			_, ch := s.sentry.Capture(packet, nil)
			if errSentry := <-ch; errSentry != nil {
				s.logger.Error(errSentry)
			}
		} else if recovered := recover(); recovered != nil {
			s.logger.Error(recovered)
		}
	}()
	s.wg.Add(1)
	defer s.wg.Done()
	if handler, found := s.handlers[ev.Name]; found {
		return (*handler)(ev.Args)
	}

	return nil, ErrNoTaskHandler
}

func (s *Server) RegistedHandlers() []string {
	var res []string
	for name, _ := range s.handlers {
		res = append(res, name)
	}
	return res
}

func (s *Server) listen() error {
	workerSocket, err := s.context.NewSocket(zmq.REP)
	if err != nil {
		return err
	}

	if err := workerSocket.Connect(DEALER_ENDPOINT); err != nil {
		return err
	}
	defer workerSocket.Close()
	var responseEvent *Event
	var identity string
	for {
		startT := time.Now()
		barr, err := workerSocket.RecvMessageBytes(0)
		rev := len(barr)
		if err != nil {
			if ErrorEqual(err, zmq.ETERM) || ErrorEqual(err, syscall.EINTR) {
				return err
			}
			responseEvent, _ = newEvent("ERR", []interface{}{err.Error(), nil, nil})
			ret, _ := sendEvent(workerSocket, responseEvent, identity)
			if s.logger != nil {
				s.logger.Infof("Unknown\t%d\t%d\t%d\t%s", rev, ret, time.Now().Sub(startT), err.Error())
			}
			continue
		}
		if len(barr) > 1 {
			identity = string(barr[0])
		}
		ev, err := unPackBytes(barr[len(barr)-1])
		if err != nil {
			responseEvent, _ = newEvent("ERR", []interface{}{err.Error(), nil, nil})
			ret, _ := sendEvent(workerSocket, responseEvent, identity)
			if s.logger != nil {
				s.logger.Infof("Unknown\t%d\t%d\t%d\t%s", rev, err.Error(), ret, time.Now().Sub(startT), err.Error())
			}
			continue
		}
		r, err := s.handleTask(ev)
		if err != nil {
			responseEvent, _ = newEvent("ERR", []interface{}{err.Error(), nil, nil})
			ret, _ := sendEvent(workerSocket, responseEvent, identity)
			if s.logger != nil {
				s.logger.Infof("%s\t%d\t%d\t%d\t%s", ev.Name, rev, ret, time.Now().Sub(startT), err.Error())
			}
			continue
		}
		if ev.isBlackHole() {
			r = nil
		}

		responseEvent, err = newEvent("OK", []interface{}{r})
		if err != nil {
			responseEvent, _ = newEvent("ERR", []interface{}{err.Error(), nil, nil})
		}
		ret, _ := sendEvent(workerSocket, responseEvent, identity)
		if s.logger != nil {
			switch responseEvent.Name {
			case "OK":
				s.logger.Infof("%s\t%d\t%d\t%d\tOK", ev.Name, rev, ret, time.Now().Sub(startT))
			default:
				s.logger.Infof("%s\t%d\t%d\t%d\t%s", ev.Name, rev, ret, time.Now().Sub(startT), err.Error())
			}
		}
	}
	return nil
}

func sendEvent(workerSocket *zmq.Socket, responseEvent *Event, identity string) (int, error) {
	responseBytes, err := responseEvent.packBytes()
	if err != nil {
		return 0, err
	}
	workerSocket.SendMessage(identity, responseBytes)
	return len(responseBytes), nil
}
