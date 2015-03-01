package amqplib

import (
	"log"

	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

type PublisherType int

const (
	WorkItemPublisher PublisherType = iota
	PubSubPublisher
)

type PublisherConnInfo struct {
	Url                string
	Exchange           string
	ExchangeType       string
	ExchangeDurable    bool
	ExchangeAutoDelete bool
	RoutingKey         string
	MsgChannel         <-chan Message
}

func NewPublisherConnInfo(publisherType PublisherType) PublisherConnInfo {
	pci := PublisherConnInfo{
		ExchangeDurable:    false,
		ExchangeAutoDelete: true,
		RoutingKey:         "",
	}

	if publisherType == PubSubPublisher {
		// Durable exchange, transient unique receive queues
		pci.ExchangeDurable = true
		pci.ExchangeAutoDelete = false
		pci.ExchangeType = "fanout"
	}

	return pci
}

func PublisherSession(conninfo PublisherConnInfo) (Session, error) {
	conn, err := amqp.Dial(conninfo.Url)

	if err != nil {
		return Session{}, err
	}

	ch, err := conn.Channel()

	if err != nil {
		conn.Close()
		return Session{nil, nil}, err
	}

	if err := ch.ExchangeDeclare(
		conninfo.Exchange,           // exchange
		conninfo.ExchangeType,       // kind
		conninfo.ExchangeDurable,    // durable
		conninfo.ExchangeAutoDelete, // autodelete
		false, // internal
		false, // nowait
		nil,   // args
	); err != nil {
		conn.Close()
		return Session{nil, nil}, err
	}

	return Session{conn, ch}, nil
}

func PublisherSessionChannel(ctx context.Context, conninfo PublisherConnInfo) chan chan Session {
	sessionFactory := func() (Session, error) {
		s, err := PublisherSession(conninfo)
		return s, err
	}

	return SessionChannel(ctx, sessionFactory)
}

func Publish(conninfo PublisherConnInfo, sessions chan chan Session) {
	var (
		running   bool
		reading   = conninfo.MsgChannel
		pending   = make(chan Message, 1)
		ack, nack = make(chan uint64), make(chan uint64)
	)

	for session := range sessions {
		pub := <-session

		// publisher confirms for this channel/connection
		if err := pub.Confirm(false); err != nil {
			log.Printf("publisher confirms not supported")
			close(ack) // confirms not supported, simulate by always acking
		} else {
			pub.NotifyConfirm(ack, nack)
		}

		log.Printf("publishing...")

		for {
			var body Message
			select {
			case <-ack:
				reading = conninfo.MsgChannel

			case id := <-nack:
				log.Printf("nack message %d, body: %q", id, string(body))
				reading = conninfo.MsgChannel

			case body = <-pending:
				err := pub.Publish(
					conninfo.Exchange,   // exchange
					conninfo.RoutingKey, // routing key
					false,               // mandatory
					false,               // immediate
					amqp.Publishing{
						Body: body,
					})

				// Retry failed delivery on the next session
				if err != nil {
					pending <- body
					pub.Connection.Close()
					break
				}

			case body, running = <-reading:
				// all messages consumed
				if !running {
					return
				}
				// work on pending delivery until ack'd
				pending <- body
				reading = nil
			}
		}
	}
}

func PublishLoop(conninfo PublisherConnInfo) {
	ctx, done := context.WithCancel(context.Background())
	go func() {
		Publish(conninfo, PublisherSessionChannel(ctx, conninfo))
		done()
	}()

	<-ctx.Done()
}
