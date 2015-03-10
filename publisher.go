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

	DefaultPublisher
)

type PublisherConnInfo struct {
	Url                string
	Exchange           string
	ExchangeType       string
	ExchangeDurable    bool
	ExchangeAutoDelete bool
	Confirms           bool
	RoutingKey         string
	MsgChannel         <-chan Message
}

func NewPublisherConnInfo(publisherType PublisherType) PublisherConnInfo {
	pci := PublisherConnInfo{
		ExchangeDurable:    false,
		ExchangeAutoDelete: true,
		Confirms:           false,
		RoutingKey:         "",
	}

	if publisherType == PubSubPublisher {
		// Durable exchange, transient unique receive queues
		pci.ExchangeDurable = true
		pci.ExchangeAutoDelete = false
		pci.ExchangeType = "fanout"
	}

	if publisherType == WorkItemPublisher {
		// Durable exchange, durable receive queues, with confirm
		pci.ExchangeDurable = true
		pci.ExchangeAutoDelete = false
		pci.ExchangeType = "fanout"
		pci.Confirms = true
	}

	return pci
}

func NewPubSubPublisher(exchange string) PublisherConnInfo {
	pci := NewPublisherConnInfo(PubSubPublisher)

	pci.Exchange = exchange
	return pci
}

func NewWorkItemPublisher(exchange string) PublisherConnInfo {
	pci := NewPublisherConnInfo(WorkItemPublisher)

	pci.Exchange = exchange
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
		ack, nack chan uint64
		pubIndex  uint64
		body      Message
	)

	for {
		pubIndex = 0
		log.Printf("Fetching new publish session")
		session := <-sessions
		pub := <-session

		ack, nack = make(chan uint64), make(chan uint64)

		// publisher confirms for this channel/connection
		if err := pub.Confirm(false); err != nil || !conninfo.Confirms {
			log.Printf("disabling publisher confirms...")
			close(ack) // confirms not supported, simulate by always acking
		} else {
			pub.NotifyConfirm(ack, nack)
		}

		log.Printf("Starting publish loop")

	outer:
		for {
			select {
			case id, running := <-ack:
				if !running {
					if reading == nil {
						pending <- body
					}
					break outer
				}

				if id == pubIndex {
					reading = conninfo.MsgChannel
				} else {
					// Maybe we've come de-synced from server?
					log.Printf("gratuitous ack")
				}

			case id, running := <-nack:
				if !running {
					if reading == nil {
						log.Printf("Re-queuing lost message")
						pending <- body
					}
					break outer
				}

				if id == pubIndex {
					log.Printf("Re-sending message %s", body)
					pending <- body
					reading = nil
				} else {
					// This might want a recycle
					log.Printf("gratuitous nack")
				}

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
					reading = nil
					break outer
				}

				pubIndex++

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

		log.Printf("Recycling connection")
		pub.Channel.Close()
		pub.Connection.Close()
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
