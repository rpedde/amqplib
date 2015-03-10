package main

import (
	"flag"
	"log"
	"math/rand"
	"time"

	"github.com/rpedde/amqplib"
)

func recv(message amqplib.Message) bool {
	num := rand.Intn(3) + 1

	log.Printf("got %s: sleeping %s", message, num)
	time.Sleep(time.Duration(num) * time.Second)
	log.Printf("finished %s", message)
	return true
}

func main() {
	rand.Seed(time.Now().Unix())

	ci := amqplib.NewWorkItemReceiver("worker-test-exchange", "worker-test-queue")

	flag.StringVar(&ci.Url, "url", "amqp:///", "AMQP url")
	flag.Parse()

	ci.Callback = recv
	ci.Receivers = 3

	amqplib.ReceiveLoop(ci)
}
