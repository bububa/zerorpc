package zerorpc

import (
	"github.com/samuel/go-zookeeper/zk"
	"math/rand"
	"sync"
)

type Nodes struct {
	items []string
	sync.RWMutex
}
type Cluster struct {
	nodes  *Nodes
	zkConn *zk.Conn
	zkPath string
	exitCh chan struct{}
	nodeCh <-chan zk.Event
}

func NewNodes() *Nodes {
	return &Nodes{}
}

func (this *Nodes) RandNode() string {
	this.RLock()
	defer this.RUnlock()
	total := len(this.items)
	if total == 0 {
		return ""
	}
	return this.items[randNum(0, total)]
}

func (this *Nodes) Update(nodes []string) {
	this.Lock()
	this.Unlock()
	this.items = nodes
}

func NewCluster(zkConn *zk.Conn, zkPath string) *Cluster {
	return &Cluster{
		nodes:  NewNodes(),
		zkPath: zkPath,
		zkConn: zkConn,
		nodeCh: make(chan zk.Event),
	}
}

func (c *Cluster) Connect() {
	c.getNodes()
	go WatchChildren(c.zkConn, c.zkPath, c.updateNodes)
}

func (c *Cluster) Close() {
	c.zkConn.Close()
	c.nodes.Update([]string{})
}

func (c *Cluster) getNodes() error {
	nodes, _, err := c.zkConn.Children(c.zkPath)
	if err != nil {
		return err
	}
	c.updateNodes(nodes)
	return nil
}

func (c *Cluster) updateNodes(nodes []string) {
	c.nodes.Update(nodes)
}

func (c *Cluster) RandNode() string {
	return c.nodes.RandNode()
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
