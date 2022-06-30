package seq

import (
	"fmt"
	"testing"

	qualiapb "github.com/tachunwu/qualia/pkg/proto/qualia"
)

func TestSeq1MTxn(t *testing.T) {
	// Start seq
	seq := NewSequencer("0")
	go seq.SubscribeFromGlobalLog()
	go func() {
		for i := 0; i < 1000000; i++ {
			seq.ReceiveNewTxn(&qualiapb.Txn{
				TxnId: uint64(i),
			})
		}
	}()
	go func() {
		for i := 1000001; i < 2000000; i++ {
			seq.ReceiveNewTxn(&qualiapb.Txn{
				TxnId: uint64(i),
			})
		}
	}()

	for i := 0; i < 2000000; i++ {
		fmt.Println(seq.GetNextTxnFromGlobalLog())
	}

}
