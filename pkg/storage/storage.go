package storage

import (
	"context"

	qualiapb "github.com/tachunwu/qualia/pkg/proto/qualia"
)

type Storage interface {
	Init(dir string) error
	Get(context.Context, []byte) (*qualiapb.KeyValue, error)
	Set(context.Context, *qualiapb.KeyValue) error
	Del(context.Context, []byte) error
}
