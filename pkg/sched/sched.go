package sched

import (
	"container/list"
	"sync"

	qualiapb "github.com/tachunwu/qualia/pkg/proto/qualia"
)

var TxnQueueSizeLimit int = 4096

type Sched struct {
	mu              sync.Mutex
	data            map[*qualiapb.KeyEntry]Value
	txnQueue        TxnQueue
	newTxnRequestCh chan *qualiapb.Txn
	executeTxnCh    chan *qualiapb.Txn
}

func NewSched() *Sched {
	return &Sched{
		data:            make(map[*qualiapb.KeyEntry]Value),
		newTxnRequestCh: make(chan *qualiapb.Txn),
		executeTxnCh:    make(chan *qualiapb.Txn),
	}
}

func (sched *Sched) GetNewTxnRequest() *qualiapb.Txn { return &qualiapb.Txn{} }

type Value struct {
	value []byte
	cs    uint64
	cx    uint64
}

type TxnQueue struct {
	queue             *list.List
	txnQueueSizeLimit int
}

func NewTxnQueue() *TxnQueue {
	q := list.New()
	return &TxnQueue{
		queue:             q,
		txnQueueSizeLimit: TxnQueueSizeLimit,
	}
}

func (tq *TxnQueue) Enqueue(t *qualiapb.Txn) {
	tq.queue.PushBack(t)
}
func (tq *TxnQueue) Remove(t *qualiapb.Txn) {
	e := &list.Element{
		Value: t,
	}
	tq.queue.Remove(e)
}
func (tq *TxnQueue) Front() *qualiapb.Txn { return tq.queue.Front().Value.(*qualiapb.Txn) }
func (tq *TxnQueue) IsNotFull() bool {
	return tq.queue.Len() < TxnQueueSizeLimit
}
