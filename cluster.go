package zerorpc

import (
	"container/list"
	"github.com/samuel/go-zookeeper/zk"
	"sync"
)

type Nodes struct {
	elements *list.List
	items    map[string]*list.Element
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
	return &Nodes{elements: list.New(), items: make(map[string]*list.Element)}
}

func (this *Nodes) Get() string {
	this.Lock()
	defer this.Unlock()
	if len(this.items) == 0 {
		return ""
	}
	element := this.elements.Front()
	this.elements.MoveToBack(element)
	return element.Value.(string)
}

func (this *Nodes) Update(nodes []string) {
	this.Lock()
	defer this.Unlock()
	for _, node := range nodes {
		if _, ok := this.items[node]; ok {
			continue
		}
		element := this.elements.PushFront(node)
		this.items[node] = element
	}
	var oldNodes []string
	for node, _ := range this.items {
		oldNodes = append(oldNodes, node)
	}
	nodeSet := NewStringSet(nodes...)
	removeNodeSet := NewStringSet(oldNodes...).Diff(nodeSet)
	for _, node := range removeNodeSet {
		if element, found := this.items[node]; found {
			this.elements.Remove(element)
			delete(this.items, node)
		}
	}
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

func (c *Cluster) GetNode() string {
	return c.nodes.Get()
}
