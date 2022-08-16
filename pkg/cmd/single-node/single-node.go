package main

import (
	"log"
	"runtime"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func main() {

	// Start embedded NATS
	ns := StartEmbeddedNATS()

	// Create Calvin transaction stream
	CreateCalvinTransactionStream(ns)
	// Start transmitter consumer
	StartTransmitter(ns)

	// Start pebble snapshot consumer
	StartSnapshot(ns)

	// Start qualia API server

	// Test Publish to transmitter
	PublishToTransaction(ns)

	for {
		runtime.Gosched()
	}
}

func CreateCalvinTransactionStream(ns *server.Server) {
	nc, err := nats.Connect(ns.ClientURL())

	if err != nil {
		log.Println(err)
	}

	js, _ := nc.JetStream()
	js.AddStream(&nats.StreamConfig{
		Name:     "TRANSACTION",
		Subjects: []string{"transaction"},
	})
}

func StartEmbeddedNATS() *server.Server {
	opts := &server.Options{
		JetStream: true,
		StoreDir:  "./jetstream",
		Trace:     true,
		NoLog:     false,
	}
	ns, err := server.NewServer(opts)

	if err != nil {
		panic(err)
	}

	go ns.Start()

	// Wait for server to be ready for connections
	if !ns.ReadyForConnections(4 * time.Second) {
		panic("not ready for connection")
	}

	log.Println("Start NATS embedded server")
	return ns
}

func StartTransmitter(ns *server.Server) {
	nc, err := nats.Connect(ns.ClientURL())

	if err != nil {
		log.Println(err)
	}

	js, _ := nc.JetStream()

	js.AddConsumer("TRANSACTION", &nats.ConsumerConfig{
		Durable: "TRANSMITTER",
	})

	subject := "transaction"

	// Subscribe to the subject
	nc.Subscribe(subject, func(msg *nats.Msg) {
		// Print message data
		data := string(msg.Data)
		log.Println("Transmitter process: ", data)
		msg.Ack()
	})
	log.Println("Start Transmitter consumer")
}

func PublishToTransaction(ns *server.Server) {
	nc, err := nats.Connect(ns.ClientURL())

	if err != nil {
		log.Println(err)
	}

	js, _ := nc.JetStream()

	// Publish messages asynchronously.
	for i := 0; i < 10; i++ {
		log.Println("Publish to transaction")
		js.PublishAsync("transaction", []byte("Publish to transaction"))
	}
}

func StartSnapshot(ns *server.Server) {
	nc, err := nats.Connect(ns.ClientURL())

	if err != nil {
		log.Println(err)
	}

	js, _ := nc.JetStream()
	js.AddConsumer("TRANSACTION", &nats.ConsumerConfig{
		Durable: "SNAPSHOT",
	})

	subject := "transaction"

	// Subscribe to the subject
	nc.Subscribe(subject, func(msg *nats.Msg) {
		// Print message data
		data := string(msg.Data)
		log.Println("Snapshot process: ", data)
		msg.Ack()
	})
	log.Println("Start Snapshot consumer")
}
