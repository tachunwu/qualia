package sched

import (
	qualiapb "github.com/tachunwu/qualia/pkg/proto/qualia"
)

func (sched *Sched) BeginTxn(t *qualiapb.Txn) {
	/* <begin critical section> */
	sched.mu.Lock()

	t.TxnType = qualiapb.Txn_FREE
	for _, key := range t.ReadSet {
		data := sched.data[key]
		data.cs++
		sched.data[key] = data

		if data.cx > 0 {
			t.TxnType = qualiapb.Txn_BLOCKED
		}
	}

	for _, key := range t.WriteSet {
		data := sched.data[key]
		data.cx++
		sched.data[key] = data

		if data.cx > 1 || data.cs > 0 {
			t.TxnType = qualiapb.Txn_BLOCKED
		}
	}

	sched.txnQueue.Enqueue(t)
	/* <end critical section> */
	sched.mu.Unlock()
}

func (sched *Sched) FinishTxn(t *qualiapb.Txn, fromFront bool) {
	/* <begin critical section> */
	sched.mu.Lock()

	for _, key := range t.ReadSet {
		data := sched.data[key]
		data.cs--
		sched.data[key] = data
	}

	for _, key := range t.WriteSet {
		data := sched.data[key]
		data.cx--
		sched.data[key] = data
	}
	if fromFront {
		sched.txnQueue.RemoveFront()
	} else {
		sched.txnQueue.RemoveBack()
	}

	/* <end critical section> */
	sched.mu.Unlock()
}

func (sched *Sched) VLL() {
	for {
		if sched.txnQueue.Len() != 0 && sched.txnQueue.Front().TxnType == qualiapb.Txn_BLOCKED {

			t := sched.txnQueue.Front()
			t.TxnType = qualiapb.Txn_FREE
			// TODO: Execute txn

			sched.FinishTxn(t, true)

		} else if sched.txnQueue.IsNotFull() {

			// TODO: GetNewTxnRequest()
			t := <-sched.newTxnRequestCh
			if t != nil {

				sched.BeginTxn(t)
				if t.TxnType == qualiapb.Txn_FREE {
					// TODO: Execute txn
					// log.Println("txn execute", t.TxnId)

					sched.FinishTxn(t, false)
				}
			}

		}
	}
}
