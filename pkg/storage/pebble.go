package storage

import (
	"context"

	"github.com/cockroachdb/pebble"
	qualiapb "github.com/tachunwu/qualia/pkg/proto/qualia"
)

type pebbleStorage struct {
	db *pebble.DB
}

func (s *pebbleStorage) Init(dir string) error {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *pebbleStorage) Get(ctx context.Context, key []byte) (*qualiapb.KeyValue, error) {
	val, closer, err := s.db.Get(key)
	defer closer.Close()

	if err != nil {
		return nil, err
	}

	return &qualiapb.KeyValue{
		Key:   key,
		Value: val,
	}, nil
}

func (s *pebbleStorage) Set(ctx context.Context, kv *qualiapb.KeyValue) error {
	if err := s.db.Set(kv.GetKey(), kv.GetValue(), pebble.Sync); err != nil {
		return err
	}
	return nil
}

func (s *pebbleStorage) Del(ctx context.Context, key []byte) error {
	return s.db.Delete(key, pebble.Sync)
}
