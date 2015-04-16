package zerorpc

import (
	"errors"
	"fmt"
	"github.com/getsentry/raven-go"
	log "github.com/kdar/factorlog"
	zmq "github.com/pebbe/zmq4"
)

// ZeroRPC server representation,
// it holds a pointer to the ZeroMQ socket
type Server struct {
	context      *zmq.Context
	routerSocket *zmq.Socket
	dealerSocket *zmq.Socket
	maxWorkers   int
	logger       *log.FactorLog
	sentry       *raven.Client
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

func NewServer(endpoint string, maxWorkers int) (*Server, error) {
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

func (s *Server) Run() {
	for i := 0; i < s.maxWorkers; i++ {
		go s.listen()
	}

	zmq.Proxy(s.routerSocket, s.dealerSocket, nil)
}

// Closes the ZeroMQ socket
func (s *Server) Close() {
	s.routerSocket.Close()
	s.dealerSocket.Close()
	s.context.Term()
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
		barr, err := workerSocket.RecvMessageBytes(0)
		rev := len(barr)
		if err != nil {
			responseEvent, _ = newEvent("ERR", []interface{}{err.Error(), nil, nil})
			ret, _ := sendEvent(workerSocket, responseEvent, identity)
			if s.logger != nil {
				s.logger.Infof("Unknown\t%d\t%d\t%s", rev, ret, err.Error())
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
				s.logger.Infof("Unknown\t%d\t%d\t%s", rev, err.Error(), ret, err.Error())
			}
			continue
		}
		r, err := s.handleTask(ev)
		if err != nil {
			responseEvent, _ = newEvent("ERR", []interface{}{err.Error(), nil, nil})
			ret, _ := sendEvent(workerSocket, responseEvent, identity)
			if s.logger != nil {
				s.logger.Infof("%s\t%d\t%d\t%s", ev.Name, rev, ret, err.Error())
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
				s.logger.Infof("%s\t%d\t%d\tOK", ev.Name, rev, ret)
			default:
				s.logger.Infof("%s\t%d\t%d\t%s", ev.Name, rev, ret, err.Error())
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
