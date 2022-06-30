package seq

import (
	"fmt"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	qualiapb "github.com/tachunwu/qualia/pkg/proto/qualia"
	"google.golang.org/protobuf/proto"
)

var natsCluster = "nats://localhost:4000, nats://localhost:4001, nats://localhost:4002, nats://localhost:4003, nats://localhost:4004, nats://localhost:4005"

type Sequencer interface {
	ReceiveNewTxn()
}

type sequencer struct {
	replicaId string
	ns        *server.Server
	seqWriter nats.JetStreamContext
	seqReader *nats.Subscription
	storeDir  string
	txnCh     chan *qualiapb.Txn
}

func NewSequencer(replicaId string) *sequencer {

	// Connect to server
	nc, err := nats.Connect(natsCluster)
	if err != nil {
		panic(err)
	}
	js, _ := nc.JetStream()

	// Create a stream for this replica
	js.AddStream(&nats.StreamConfig{
		Name:     "QUALIA",
		Subjects: []string{"QUALIA.*"},
	})

	// Create a consumer for this replica
	js.AddConsumer("QUALIA", &nats.ConsumerConfig{
		Durable: replicaId,
	})

	// Pull subscribe global log consumer
	seqReader, err := js.PullSubscribe("QUALIA.*", replicaId)
	if err != nil {
		panic(err)
	}

	// Create seqWriter
	seqWriter, err := nc.JetStream()
	if err != nil {
		panic(err)
	}
	return &sequencer{
		seqReader: seqReader,
		seqWriter: seqWriter,
		txnCh:     make(chan *qualiapb.Txn),
	}
}

func (seq *sequencer) ReceiveNewTxn(txn *qualiapb.Txn) {
	// Check is single/multi home txn

	// Insert to log
	t, err := proto.Marshal(txn)
	if err != nil {
		fmt.Println(err)
	}
	seq.seqWriter.PublishAsync("QUALIA.*", t)
	select {
	case <-seq.seqWriter.PublishAsyncComplete():
		// fmt.Println("Enqueue txn success")
	case <-time.After(5 * time.Second):
		// fmt.Println("Enqueue txn timeout")
	}
}

func (seq *sequencer) GetNextTxnFromGlobalLog() *qualiapb.Txn {
	return <-seq.txnCh
}

func (seq *sequencer) SubscribeFromGlobalLog() {
	for {
		txns, err := seq.seqReader.Fetch(1024)
		if err != nil {
			fmt.Println(err)
		}
		for _, txn := range txns {
			t := &qualiapb.Txn{}
			proto.Unmarshal(txn.Data, t)

			seq.txnCh <- t
			txn.Ack()

		}
	}
}

func EmbedJetstream() {
	// Start embed jetsteam server
	opts := &server.Options{
		JetStream: true,
		StoreDir:  "./data",
		Cluster:   server.ClusterOpts{},
	}

	ns, err := server.NewServer(opts)

	if err != nil {
		panic(err)
	}

	go ns.Start()

	if !ns.ReadyForConnections(4 * time.Second) {
		panic("not ready for connection")
	}

}
