package sched

import (
	"testing"

	qualiapb "github.com/tachunwu/qualia/pkg/proto/qualia"
)

func TestSched1MTxn(t *testing.T) {
	sched := NewSched()
	go sched.VLL()
	for i := 0; i < 1000000; i++ {
		t := &qualiapb.Txn{
			TxnId:   uint64(i),
			TxnType: qualiapb.Txn_FREE,
		}
		sched.newTxnRequestCh <- t
	}
}
