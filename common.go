package amqplib

import (
	"log"

	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

type Message []byte

type Session struct {
	*amqp.Connection
	*amqp.Channel
}

func SessionChannel(ctx context.Context, sessionFactory func() (Session, error)) chan chan Session {
	sessions := make(chan chan Session)

	go func() {
		sess := make(chan Session)
		defer close(sessions)

		for {
			select {
			case sessions <- sess:
			case <-ctx.Done():
				log.Println("Shutting down sesson factory")
				return
			}

			session, err := sessionFactory()
			if err != nil {
				// we should do slow retries here
				log.Fatalf("Error creating session: %v", err)
			}

			select {
			case sess <- session:
			case <-ctx.Done():
				log.Println("Shutting down session factory")
				return
			}
		}
	}()

	return sessions
}
