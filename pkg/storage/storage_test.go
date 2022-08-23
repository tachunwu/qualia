package storage

import (
	"context"
	"log"
	"testing"

	"github.com/tachunwu/qualia/pkg/proto/qualia"
)

func TestStorage(t *testing.T) {
	pebble := &pebbleStorage{}
	err := pebble.Init("./data")

	if err != nil {
		log.Println(err)
	}
	ctx := context.TODO()
	key := []byte("hello")
	val := []byte("world")

	kv := &qualia.KeyValue{
		Key:   key,
		Value: val,
	}
	pebble.Set(ctx, kv)

	res, err := pebble.Get(ctx, key)
	if err != nil {
		log.Println(res)
	} else {
		log.Println(res)
	}

	pebble.Del(ctx, key)
}
