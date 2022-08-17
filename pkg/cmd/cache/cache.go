package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"runtime"

	"github.com/dgraph-io/ristretto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/pflag"
	qualiapb "github.com/tachunwu/qualia/pkg/proto/qualia"
	"google.golang.org/protobuf/proto"
)

type Value []byte

var addr = pflag.String("addr", "", "Local stream addr")

func main() {
	log.SetOutput(ioutil.Discard)
	pflag.Parse()
	c := NewCache(*addr)
	c.Serve()

	log.Println("Cache start serving, link to nats at: ", *addr)
	for {
		runtime.Gosched()
	}
}

type Cache struct {
	kv   *ristretto.Cache
	addr string
}

func NewCache(addr string) *Cache {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})

	if err != nil {
		log.Println(err)
		return nil
	}
	return &Cache{
		kv:   cache,
		addr: addr,
	}

}

func (c *Cache) Serve() {
	nc, err := nats.Connect(c.addr)
	if err != nil {
		log.Println(err)
	}

	nc.Subscribe("cache.get", func(m *nats.Msg) {
		log.Println("Cache process Get")
		req := &qualiapb.GetRequest{}
		err = proto.Unmarshal(m.Data, req)

		// Cache get
		value, found := c.kv.Get(req.Key)
		if !found {
			nc.Publish(m.Reply, []byte("not found in cache"))
		}
		fmt.Println(value)
		res := &qualiapb.GetResponse{
			// Value: value.(Value),
		}

		data, err := proto.Marshal(res)
		if err != nil {
			log.Println(err)
		}
		nc.Publish(m.Reply, data)
	})

	nc.Subscribe("cache.set", func(m *nats.Msg) {
		log.Println("Cache process Set")
		req := &qualiapb.SetRequest{}
		err := proto.Unmarshal(m.Data, req)
		if err != nil {
			log.Println(err)
		}

		// Cache set
		c.kv.Set(req.Key, req.Value, 1)
		res := &qualiapb.SetResponse{
			Status: "set success",
		}
		data, err := proto.Marshal(res)
		if err != nil {
			log.Println(err)
		}
		nc.Publish(m.Reply, data)
	})

	nc.Subscribe("cache.del", func(m *nats.Msg) {
		log.Println("Cache process Del")
		req := &qualiapb.DelRequest{}
		err := proto.Unmarshal(m.Data, req)
		if err != nil {
			log.Println(err)
		}

		// Cache set
		c.kv.Del(req.Key)
		res := &qualiapb.DelResponse{
			Status: "del success",
		}
		data, err := proto.Marshal(res)
		if err != nil {
			log.Println(err)
		}
		nc.Publish(m.Reply, data)
	})

}
