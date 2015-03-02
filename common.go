package amqplib

import (
	"log"
	"time"

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

	var firstCreate = true
	var maxInterval = time.Duration(time.Second * 300)

	go func() {
		var session Session
		var err error

		sess := make(chan Session)
		defer close(sessions)

		for {
			select {
			case sessions <- sess:
			case <-ctx.Done():
				log.Println("Shutting down sesson factory")
				return
			}

			retryInterval, _ := time.ParseDuration("5s")

			for {
				session, err = sessionFactory()

				if err != nil {
					// we should do slow retries here
					if firstCreate {
						log.Fatalf("Error creating session: %v", err)
					}

					// wait a while.
					log.Printf("Error connecting.  Sleeping %s", retryInterval)

					time.Sleep(retryInterval)
					retryInterval = (time.Duration(retryInterval * 2))
					if retryInterval > maxInterval {
						retryInterval = maxInterval
					}
				} else {
					break
				}
			}

			firstCreate = false

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
