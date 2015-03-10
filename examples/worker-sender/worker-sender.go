package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/rpedde/amqplib"
)

func send(what string, howlong int) <-chan amqplib.Message {
	lines := make(chan amqplib.Message)
	go func() {
		var count int

		for {
			defer close(lines)
			msg := fmt.Sprintf("job-%d", count)
			lines <- amqplib.Message(msg)
			count++
			log.Printf("Sent: %s\n", msg)
			time.Sleep(time.Duration(howlong) * time.Second)
		}
	}()

	return lines
}

func main() {
	ci := amqplib.NewWorkItemPublisher("worker-test-exchange")

	flag.StringVar(&ci.Url, "url", "amqp:///", "AMQP url")
	flag.Parse()

	ci.MsgChannel = send("job", 1)

	amqplib.PublishLoop(ci)
}
