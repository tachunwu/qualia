package main

import (
	qualiapb "github.com/tachunwu/qualia/pkg/proto/qualia"
	"github.com/tachunwu/qualia/pkg/seq"
)

func main() {
	seq0 := seq.NewSequencer("0")
	go seq0.SubscribeFromGlobalLog()

	go func() {
		for i := 0; i < 1000000; i++ {
			txn := &qualiapb.Txn{
				TxnId: uint64(i),
			}
			seq0.ReceiveNewTxn(txn)
		}
	}()

	for i := 0; i < 1000000; i++ {
		seq0.GetNextTxnFromGlobalLog()
	}

}
