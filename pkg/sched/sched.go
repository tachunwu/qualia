package sched

import (
	"container/list"
	"sync"

	qualiapb "github.com/tachunwu/qualia/pkg/proto/qualia"
	"go.uber.org/zap"
)

var TxnQueueSizeLimit int = 4096

type Sched struct {
	mu              sync.Mutex
	data            map[*qualiapb.KeyEntry]Value
	txnQueue        *TxnQueue
	newTxnRequestCh chan *qualiapb.Txn
	executeTxnCh    chan *qualiapb.Txn
	log             *zap.Logger
}

func NewSched() *Sched {
	log, _ := zap.NewProduction()
	return &Sched{
		txnQueue:        NewTxnQueue(),
		data:            make(map[*qualiapb.KeyEntry]Value),
		newTxnRequestCh: make(chan *qualiapb.Txn, 1),
		executeTxnCh:    make(chan *qualiapb.Txn, 1),
		log:             log,
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

func (tq *TxnQueue) RemoveFront() {
	tq.queue.Remove(tq.queue.Front())
}

func (tq *TxnQueue) RemoveBack() {
	tq.queue.Remove(tq.queue.Back())
}
func (tq *TxnQueue) Front() *qualiapb.Txn {
	if tq.queue.Front() != nil {
		return tq.queue.Front().Value.(*qualiapb.Txn)
	}
	return nil
}
func (tq *TxnQueue) IsNotFull() bool {
	return tq.queue.Len() < TxnQueueSizeLimit
}

func (tq *TxnQueue) Len() int {
	return tq.queue.Len()
}
