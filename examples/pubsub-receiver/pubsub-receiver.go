package main

import (
	"flag"
	"log"

	"github.com/rpedde/amqplib"
)

func recv(message amqplib.Message) bool {
	log.Printf("Got message: %s\n", message)
	return true
}

func main() {
	ci := amqplib.NewPubSubReceiver("pubsub-test-exchange")

	flag.StringVar(&ci.Url, "url", "amqp:///", "AMQP url")
	flag.Parse()

	ci.Callback = recv

	amqplib.ReceiveLoop(ci)
}
