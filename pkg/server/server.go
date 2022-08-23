package server

import (
	"github.com/tachunwu/qualia/pkg/storage"
	"github.com/tachunwu/qualia/pkg/transport"
)

type Server struct {
	s storage.Storage
	t transport.Transport
}
