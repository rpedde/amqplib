package amqplib

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

type ReceiverType int

const (
	WorkItemReceiver ReceiverType = iota
	PubSubReceiver
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
	MsgChannel         chan<- Message
}

func NewReceiverConnInfo(receiverType ReceiverType) ReceiverConnInfo {
	rci := ReceiverConnInfo{
		ExchangeDurable:    false,
		ExchangeAutoDelete: true,
		QueueDurable:       false,
		QueueAutoDelete:    true,
		QueueExclusive:     false,
		ConsumerTag:        "default",
		BindingKey:         "",
	}

	if receiverType == PubSubReceiver {
		// Durable exchange, transient unique receive queues
		rci.Queue = ""
		ExchangeDurable = true
		ExchangeAutoDelete = false
	}

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

func Receive(conninfo ReceiverConnInfo, sessions chan chan Session) {
	for session := range sessions {
		recv := <-session

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
			continue
		}

		for d := range deliveries {
			conninfo.MsgChannel <- d.Body
			d.Ack(true)
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
