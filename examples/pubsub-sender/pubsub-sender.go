package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/rpedde/amqplib"
)

func send(what string, howlong int) <-chan amqplib.Message {
	var idx = 0

	lines := make(chan amqplib.Message)
	go func() {
		defer close(lines)
		for {
			idx++
			msg := fmt.Sprintf("%s %d", what, idx)
			lines <- amqplib.Message(msg)
			log.Printf("Sent message: %s", msg)

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
