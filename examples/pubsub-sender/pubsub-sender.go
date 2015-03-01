package main

import (
	"flag"
	"log"
	"time"

	"github.com/rpedde/amqplib"
)

func send(what string, howlong int) <-chan amqplib.Message {
	lines := make(chan amqplib.Message)
	go func() {
		for {
			defer close(lines)
			lines <- amqplib.Message(what)
			log.Println("Sent...")
			time.Sleep(time.Duration(howlong) * time.Second)
		}
	}()

	return lines
}

func main() {
	ci := amqplib.NewPublisherConnInfo(amqplib.PubSubPublisher)

	flag.StringVar(&ci.Url, "url", "amqp:///", "AMQP url")
	flag.Parse()

	ci.Exchange = "pubsub-test-exchange"
	ci.MsgChannel = send("test", 1)

	amqplib.PublishLoop(ci)
}
