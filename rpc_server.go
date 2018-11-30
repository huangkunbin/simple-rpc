package rpc

import (
	"errors"
	"net"
	"net/rpc"

	"log"

	"plugin.arena/lib/debug"
)

type rpcApp struct {
	lis    net.Listener
	server *rpc.Server
}

func NEW_RPC_SERVER() *rpcApp {
	return &rpcApp{
		server: rpc.NewServer(),
	}
}

func (app *rpcApp) Register(rcvr interface{}) {
	err := app.server.Register(rcvr)
	if err != nil {
		panic(err)
	}
}

func (app *rpcApp) Start(network, address string) {
	var err error
	app.lis, err = net.Listen(network, address)
	if err != nil {
		panic(err)
	}

	go app.server.Accept(app.lis)

	log.Println("rpc start...",
		"  network=", network,
		"  address=", address,
	)
}

func (app *rpcApp) Stop() {
	if app.lis == nil {
		return
	}
	app.lis.Close()
}

func (app *rpcApp) Serve(service string, args interface{}, callback func() error) (err error) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf(`RPC Serve failed
error     = %s
service   = %s
args      = %s
stack     = %s
			`,
				err,
				service,
				debug.Dump(debug.DumpStyle{Format: true, Indent: ""}, args),
				debug.StackTrace(2).Bytes(""),
			)
			err = errors.New("RPC Serve failed")
		}
	}()

	err = callback()

	return
}
