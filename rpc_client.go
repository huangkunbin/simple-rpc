package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"sync"
	"time"

	"log"
	"mycommon/myfile"

	"plugin.arena/lib/debug"
)

var (
	ErrNodeNotFound = errors.New("Node Not Found")
)

type RPC struct {
	nodeMutex sync.Mutex
	nodes     map[uint64]*RPCNode

	linkMutex sync.Mutex
	links     map[uint64]*rpc.Client
}

func NEW_RPC_CLIENT() *RPC {
	return &RPC{
		nodes: make(map[uint64]*RPCNode),
		links: make(map[uint64]*rpc.Client),
	}
}

func (client *RPC) Call(id uint64, service string, args interface{}, reply interface{}, callback func(args, reply interface{}, err error)) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Printf(`RPC Call error
error     = %s
server_id = %d
service   = %s
args      = %s
reply     = %s
stack     = %s
			`,
					err,
					id,
					service,
					debug.Dump(debug.DumpStyle{Format: true, Indent: ""}, args),
					debug.Dump(debug.DumpStyle{Format: true, Indent: ""}, reply),
					debug.StackTrace(2).Bytes(""),
				)
			}
		}()

		link, err := client.getLink(id)
		if err == nil {
			err = link.Call(service, args, reply)
			if err == io.ErrUnexpectedEOF || err == rpc.ErrShutdown {
				link.Close()
				client.delLink(id)
			}
		}

		if err != nil {
			log.Printf(`RPC Call error
error     = %s
server_id = %d
service   = %s
args      = %s
reply     = %s
stack     = %s
			`,
				err,
				id,
				service,
				debug.Dump(debug.DumpStyle{Format: true, Indent: ""}, args),
				debug.Dump(debug.DumpStyle{Format: true, Indent: ""}, reply),
				debug.StackTrace(2).Bytes(""),
			)
		}

		if callback == nil {
			return
		}

		callback(args, reply, err)

	}()
}

func (client *RPC) MergeCall(ids []uint64, service string, argss []interface{}, replys []interface{}, callback func(results []MergeCallResult)) {
	wg := new(sync.WaitGroup)
	results := make([]MergeCallResult, len(ids))

	for i, id := range ids {
		wg.Add(1)

		func(i int, id uint64, args interface{}, reply interface{}) {
			client.Call(id, service, args, reply, func(args, reply interface{}, err error) {
				results[i] = MergeCallResult{
					Request:  args,
					Response: reply,
					Error:    err,
				}
				wg.Done()
			})
		}(i, id, argss[i], replys[i])
	}

	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Printf(`RPC Merge Call failed
		error     = %s
		service   = %s
		argss     = %s
		stack     = %s
					`,
					err,
					service,
					string(debug.Dump(debug.DumpStyle{Format: true, Indent: ""}, argss)),
					debug.StackTrace(2).Bytes(""),
				)
			}
		}()

		wg.Wait()

		if callback == nil {
			return
		}

		callback(results)

	}()
}

func (client *RPC) Broadcast(service string, args interface{}, reply interface{}) {
	ids := client.GetAllNode()
	for _, id := range ids {
		client.Call(id, service, args, reply, nil)
	}
}

type MergeCallResult struct {
	Request  interface{}
	Response interface{}
	Error    error
}

func (client *RPC) getLink(id uint64) (*rpc.Client, error) {
	client.linkMutex.Lock()
	link, ok := client.links[id]
	client.linkMutex.Unlock()

	if ok {
		return link, nil
	}

	node, err := client.GetNode(id)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTimeout(node.Network, node.Address, time.Second*3)
	if err != nil {
		return nil, errors.New(fmt.Sprintf(`RPC server %d connect failed`, id))
	}

	link = rpc.NewClient(conn)

	// 防止重复连接
	client.linkMutex.Lock()
	if link2, ok := client.links[id]; ok {
		link.Close()
		link = link2
	} else {
		client.links[id] = link
	}
	client.linkMutex.Unlock()

	return link, nil
}

func (client *RPC) delLink(id uint64) {
	client.linkMutex.Lock()
	defer client.linkMutex.Unlock()
	delete(client.links, id)
}

type RPCNode struct {
	ID      uint64
	Network string
	Address string
}

func (client *RPC) StartUpdateNode() {
	log.Println("start update rpc node")

	client.updateNode()

	go func() {
		for {
			time.Sleep(30 * time.Second)
			if len(client.links) > 0 {
				client.linkMutex.Lock()
				client.links = make(map[uint64]*rpc.Client)
				client.linkMutex.Unlock()
			}
		}
	}()
}

func (client *RPC) updateNode() {
	raw := myfile.ReadConfFile("rpc_server.json")

	log.Println("rpc_server.json:", raw)

	var nodes []*RPCNode
	err := json.Unmarshal([]byte(raw), &nodes)

	if err != nil {
		panic("parse config file '" + "rpc_server.json" + "' error: " + err.Error())
	}

	for _, node := range nodes {
		client.AddNode(node)
	}
}

func (client *RPC) AddNode(node *RPCNode) {
	client.nodeMutex.Lock()
	defer client.nodeMutex.Unlock()

	if oldNode, ok := client.nodes[node.ID]; ok {
		if node.Address != oldNode.Address {
			client.delLink(oldNode.ID)
		}
	}
	client.nodes[node.ID] = node
}

func (client *RPC) GetNode(id uint64) (*RPCNode, error) {
	client.nodeMutex.Lock()
	defer client.nodeMutex.Unlock()

	if node, ok := client.nodes[id]; ok {
		return node, nil
	}

	return nil, ErrNodeNotFound
}

func (client *RPC) GetAllNode() (ids []uint64) {
	client.nodeMutex.Lock()
	defer client.nodeMutex.Unlock()

	ids = make([]uint64, 0, len(client.nodes))
	for id := range client.nodes {
		ids = append(ids, id)
	}
	return
}
