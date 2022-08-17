package main

import (
	"context"
	"log"
	"strconv"
	"testing"
	"time"

	qualiapb "github.com/tachunwu/qualia/pkg/proto/qualia"
	rpc "google.golang.org/grpc"
)

func TestCache(t *testing.T) {
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
	log.Printf("Binomial took %s", elapsed)
}
