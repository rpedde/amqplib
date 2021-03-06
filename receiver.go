package amqplib

import (
	"fmt"
	"log"
	"reflect"

	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

type ReceiverType int

const (
	WorkItemReceiver ReceiverType = iota
	PubSubReceiver

	DefaultReceiver
)

type ReceiverConnInfo struct {
	Url                string
	Exchange           string
	ExchangeType       string
	ExchangeDurable    bool
	ExchangeAutoDelete bool
	Queue              string
	QueueDurable       bool
	QueueAutoDelete    bool
	QueueExclusive     bool
	BindingKey         string
	ConsumerTag        string
	Callback           func(Message) bool
	AutoAck            bool
	Receivers          int
}

func NewReceiverConnInfo(receiverType ReceiverType) ReceiverConnInfo {
	rci := ReceiverConnInfo{
		ExchangeDurable:    false,
		ExchangeAutoDelete: true,
		QueueDurable:       false,
		QueueAutoDelete:    true,
		QueueExclusive:     false,
		AutoAck:            true,
		ConsumerTag:        "default",
		BindingKey:         "",
		Receivers:          1,
	}

	if receiverType == PubSubReceiver {
		// Durable exchange, transient unique receive queues
		rci.Queue = ""
		rci.ExchangeDurable = true
		rci.ExchangeAutoDelete = false
		rci.ExchangeType = "fanout"
	}

	if receiverType == WorkItemReceiver {
		// Durable exchange, durable recieve queue, with confirm
		rci.ExchangeDurable = true
		rci.ExchangeAutoDelete = false
		rci.QueueDurable = true
		rci.QueueAutoDelete = false
		rci.AutoAck = false
		rci.ExchangeType = "fanout"
	}

	return rci
}

func NewPubSubReceiver(exchange string) ReceiverConnInfo {
	rci := NewReceiverConnInfo(PubSubReceiver)

	rci.Exchange = exchange
	return rci
}

func NewWorkItemReceiver(exchange string, queue string) ReceiverConnInfo {
	rci := NewReceiverConnInfo(WorkItemReceiver)

	rci.Exchange = exchange
	rci.Queue = queue
	return rci
}

func ConsumerSession(rci ReceiverConnInfo) (Session, error) {
	var err error

	conn, err := amqp.Dial(rci.Url)
	if err != nil {
		return Session{}, fmt.Errorf("Dial: %s", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		return Session{}, fmt.Errorf("Channel: %s", err)
	}

	if err = channel.ExchangeDeclare(
		rci.Exchange,           // name
		rci.ExchangeType,       // type
		rci.ExchangeDurable,    // durable
		rci.ExchangeAutoDelete, // delete when complete
		false, // internal
		false, // nowait
		nil,   // arguments
	); err != nil {
		return Session{}, fmt.Errorf("ExchangeDeclare: %s", err)
	}

	queue, err := channel.QueueDeclare(
		rci.Queue,           // queue name
		rci.QueueDurable,    // durable
		rci.QueueAutoDelete, // delete when unused
		rci.QueueExclusive,  // exclusive
		false,               // nowait
		nil,                 // arguments
	)

	if err != nil {
		return Session{}, fmt.Errorf("QueueDeclare: %s", err)
	}

	if err = channel.QueueBind(
		queue.Name,     // name of queue
		rci.BindingKey, // bindingKey
		rci.Exchange,   // exchange name
		false,          // nowait
		nil,            // arguments
	); err != nil {
		return Session{}, fmt.Errorf("QueueBind: %s", err)
	}

	return Session{conn, channel}, nil
}

func ConsumerSessionChannel(ctx context.Context, conninfo ReceiverConnInfo) chan chan Session {
	sessionFactory := func() (Session, error) {
		s, err := ConsumerSession(conninfo)
		return s, err
	}

	return SessionChannel(ctx, sessionFactory)
}

func multiReceiver(conninfo ReceiverConnInfo) chan<- amqp.Delivery {
	chans := make([]chan<- amqp.Delivery, conninfo.Receivers)
	cases := make([]reflect.SelectCase, conninfo.Receivers)
	messages := make(chan amqp.Delivery)

	go func() {
		defer close(messages)

		for i := 0; i < conninfo.Receivers; i++ {
			log.Printf("Creating worker %d", i)
			chans[i] = singleReceiver(conninfo)
			cases[i].Dir = reflect.SelectSend
			cases[i].Chan = reflect.ValueOf(chans[i])
		}

		for {
			message := <-messages
			// log.Printf("got message... dispatching")
			for i := range cases {
				cases[i].Send = reflect.ValueOf(message)
			}

			reflect.Select(cases)
			// chosen, _, _ := reflect.Select(cases)
			// log.Printf("dispatched to worker %d", chosen)
		}
	}()

	return messages
}

func singleReceiver(conninfo ReceiverConnInfo) chan<- amqp.Delivery {
	messages := make(chan amqp.Delivery)
	go func() {
		log.Printf("worker started")
		for {
			defer close(messages)
			message := <-messages
			result := safeCallback(conninfo.Callback, message.Body)
			if result || conninfo.AutoAck {
				message.Ack(false)
			} else {
				message.Nack(false, true)
			}
		}
	}()

	return messages
}

func safeCallback(fn func(Message) bool, msg Message) (result bool) {
	defer func() {
		if e := recover(); e != nil {
			// should log a panic here
			result = false
		}
	}()

	return fn(msg)
}

func Receive(conninfo ReceiverConnInfo, sessions chan chan Session) {
	for session := range sessions {
		recv := <-session

		for {
			deliveries, err := recv.Consume(
				conninfo.Queue,       // queue name
				conninfo.ConsumerTag, // consumer tag
				false,                // noack
				false,                // exclusive
				false,                // nolocal
				false,                // nowait
				nil,                  //arguments
			)

			// If we error, then just grab a new session and try and reconsume
			if err != nil {
				log.Println("Error in Consume: %s", err)
				break
			}

			msg_channel := multiReceiver(conninfo)

			for d := range deliveries {
				msg_channel <- d
			}
		}
	}
}

func ReceiveLoop(conninfo ReceiverConnInfo) {
	ctx, done := context.WithCancel(context.Background())

	go func() {
		Receive(conninfo, ConsumerSessionChannel(ctx, conninfo))
		done()
	}()

	<-ctx.Done()
}
