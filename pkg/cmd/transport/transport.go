package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"runtime"
	"strconv"
	"time"

	"github.com/cespare/xxhash"
	"github.com/nats-io/nats.go"

	"google.golang.org/protobuf/proto"

	qualiapb "github.com/tachunwu/qualia/pkg/proto/qualia"
	rpc "google.golang.org/grpc"
)

func main() {
	log.SetOutput(ioutil.Discard)
	t := NewGRPC()
	peers := map[uint64]string{
		0: "localhost:4000",
		1: "localhost:4001",
		2: "localhost:4002",
		3: "localhost:4003",
	}
	t.Init("localhost:30000", peers)
	go t.Serve()

	// Test cache
	log.Println("Start transport service")
	CacheTest()
	for {
		runtime.Gosched()
	}
}

type Transport interface {
	Init(addr string, peers map[uint64]string)
	Serve()
	Close()
}

type grpc struct {
	qualiapb.CacheServiceServer
	addr  string
	peers map[uint64]string
	nc    map[uint64]*nats.Conn

	rpc        *rpc.Server
	dialCtx    context.Context
	dialCancel func()
}

func NewGRPC() Transport {
	return new(grpc)
}

func (g *grpc) Init(addr string, peers map[uint64]string) {
	g.addr = addr
	g.peers = peers

	g.dialCtx, g.dialCancel = context.WithCancel(context.Background())
	g.rpc = rpc.NewServer()
	qualiapb.RegisterCacheServiceServer(g.rpc, g)
}

func (g *grpc) Serve() {

	var lis net.Listener

	lis, err := net.Listen("tcp", g.addr)
	if err != nil {
		log.Println(err)
	}

	if err := g.rpc.Serve(lis); err != nil {
		switch err {
		case rpc.ErrServerStopped:
		default:
			log.Fatal(err)
		}
	}

	// Link to nats
	for partition, peer := range g.peers {
		nc, err := nats.Connect(peer)
		if err != nil {
			log.Println(err)
		}
		g.nc[partition] = nc
	}
}

func (g *grpc) Get(ctx context.Context, req *qualiapb.GetRequest) (*qualiapb.GetResponse, error) {
	// Hash the key
	partition := xxhash.Sum64(req.Key) % uint64(len(g.peers))
	log.Println("Start Get command key: ", string(req.Key), "hash to :", partition)

	// Pulish
	data, err := proto.Marshal(req)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	msg, err := g.nc[partition].Request("cache.get", data, time.Second)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return &qualiapb.GetResponse{
		Value: msg.Data,
	}, nil
}

func (g *grpc) Set(ctx context.Context, req *qualiapb.SetRequest) (*qualiapb.SetResponse, error) {
	// Hash the key
	partition := xxhash.Sum64(req.Key) % uint64(len(g.peers))
	log.Println("Start Set command key: ", string(req.Key), "hash to :", partition)

	// Pulish
	data, err := proto.Marshal(req)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	msg, err := g.nc[partition].Request("cache.set", data, time.Second)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return &qualiapb.SetResponse{
		Status: string(msg.Data),
	}, nil
}

func (g *grpc) Del(ctx context.Context, req *qualiapb.DelRequest) (*qualiapb.DelResponse, error) {
	// Hash the key
	partition := xxhash.Sum64(req.Key) % uint64(len(g.peers))
	log.Println("Start Del command key: ", string(req.Key), "hash to :", partition)

	// Pulish
	data, err := proto.Marshal(req)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	msg, err := g.nc[partition].Request("cache.del", data, time.Second)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return &qualiapb.DelResponse{
		Status: string(msg.Data),
	}, nil
}

func (g *grpc) Close() {
	g.rpc.Stop()
	g.dialCancel()

	for _, peer := range g.nc {
		peer.Close()
	}
}

func CacheTest() {
	reqSize := 100000

	conn, err := rpc.Dial("localhost:30000", rpc.WithInsecure())
	if err != nil {
		switch err {
		case context.Canceled:
			return
		default:
			log.Println("error when dialing: ", err)
		}
	}

	defer conn.Close()
	c := qualiapb.NewCacheServiceClient(conn)
	start := time.Now()
	// Set
	for i := 0; i < reqSize; i++ {
		res, err := c.Set(context.Background(),
			&qualiapb.SetRequest{
				Key:   []byte(strconv.Itoa(i)),
				Value: []byte(strconv.Itoa(i)),
			})
		if err != nil {
			log.Println("failed to call Set: ", err)
		}
		log.Println(res)
	}

	// Get
	for i := 0; i < reqSize; i++ {
		res, err := c.Get(context.Background(),
			&qualiapb.GetRequest{
				Key: []byte(strconv.Itoa(i)),
			})
		if err != nil {
			log.Println("failed to call Get: ", err)
		}
		log.Println(res)
	}

	// Del

	for i := 0; i < reqSize; i++ {
		res, err := c.Del(context.Background(),
			&qualiapb.DelRequest{
				Key: []byte(strconv.Itoa(i)),
			})
		if err != nil {
			log.Println("failed to call Del: ", err)
		}
		log.Println(res)
	}
	elapsed := time.Since(start)
	fmt.Println(reqSize*3, " took ", elapsed)
}
