package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/rpedde/amqplib"
)

func recv(message amqplib.Message) bool {
	fmt.Printf("Got message: %s... ", message)
	num := rand.Intn(3)

	if num == 0 {
		fmt.Printf("panic\n")
		panic("oh noes!")
	} else if num == 1 {
		fmt.Printf("failed\n")
		return false
	} else {
		fmt.Printf("succeeded\n")
		return true
	}
}

func main() {
	rand.Seed(time.Now().Unix())

	ci := amqplib.NewWorkItemReceiver("worker-test-exchange", "worker-test-queue")

	flag.StringVar(&ci.Url, "url", "amqp:///", "AMQP url")
	flag.Parse()

	ci.Callback = recv

	amqplib.ReceiveLoop(ci)
}
