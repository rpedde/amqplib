package main

import (
	"flag"
	"log"

	"github.com/rpedde/amqplib"
)

func recv() chan<- amqplib.Message {
	messages := make(chan amqplib.Message)
	go func() {
		for {
			defer close(messages)
			message := <-messages
			log.Printf("Got message: %s\n", message)
		}
	}()

	return messages
}

func main() {
	ci := amqplib.NewReceiverConnInfo(amqplib.PubSubReceiver)

	flag.StringVar(&ci.Url, "url", "amqp:///", "AMQP url")
	flag.Parse()

	ci.Exchange = "pubsub-test-exchange"
	ci.MsgChannel = recv()

	amqplib.ReceiveLoop(ci)
}
