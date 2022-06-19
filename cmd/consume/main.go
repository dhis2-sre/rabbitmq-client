package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/dhis2-sre/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx); err != nil {
		fmt.Printf("exit due to: %s", err)
		os.Exit(1)
	}
}

type temporary interface {
	Temporary() bool
}

func run(ctx context.Context) error {
	queue := "queue_client_example"
	addr := "amqp://guest:guest@localhost:18080"
	consumer, err := rabbitmq.NewConsumer(addr, rabbitmq.WithConsumerPrefix("queue_client_example"))
	if err != nil {
		return err
	}
	fmt.Println("Connected, login to management console at http://localhost:15672/ using user/pw guest")
	defer func() {
		fmt.Println("Closing consumer")
		consumer.Close()
	}()

	for i := 0; i < 3; i++ {
		fmt.Printf("Trying to consume from queue %q at %q\n", queue, addr)
		rand.Seed(time.Now().UnixNano())

		ct, err := consumer.Consume(queue, func(d amqp.Delivery) {
			if randNack() {
				fmt.Printf("received (Nack): %s\n", string(d.Body))
				_ = d.Nack(false, false) // to provoke dead-lettering
			} else {
				fmt.Printf("received (Ack): %s\n", string(d.Body))
				_ = d.Ack(false)
			}
		})
		if err == nil {
			fmt.Printf("Consume succeeded, consumer tag %q\n", ct)
			break
		}

		var terr temporary
		if !errors.As(err, &terr) {
			return fmt.Errorf("error is not temporary: %w", err)
		}

		fmt.Printf("Consume failed: %s\n", err)
		time.Sleep(time.Second * 5)
	}

	go func() {
		c, err := amqp.Dial(addr)
		if err != nil {
			fmt.Printf("Error connecting to %q: %v\n", addr, err)
			return
		}
		ch, err := c.Channel()
		if err != nil {
			fmt.Printf("Error opening channel: %v\n", err)
			return
		}

		// TODO trying to use the direct exchange for DLX as well
		// this would simplify things as I would not have to adapt the
		// queue client

		// err = ch.ExchangeDeclare(
		// 	"dlx_queue_client_example",
		// 	"direct",
		// 	true,  // durable
		// 	false, // auto-delete
		// 	false, // internal
		// 	false, // nowait
		// 	nil,
		// )
		// if err != nil {
		// 	fmt.Printf("Error creating dlx exchange %q: %v\n", "dlx_queue_client_example", err)
		// 	return
		// }

		_, err = ch.QueueDeclare(
			"dlx_queue_client_example",
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // nowait
			nil,
		)
		if err != nil {
			fmt.Printf("Error creating dlx queue %q: %v\n", "dlx_queue_client_example", err)
			return
		}

		// err = ch.QueueBind(
		// 	"dlx_queue_client_example",
		// 	queue,
		// 	"dlx_queue_client_example",
		// 	false, // nowait
		// 	nil,
		// )
		// if err != nil {
		// 	fmt.Printf("Error binding dlx queue %q to exchange %q: %v\n", "dlx_queue_client_example", "dlx_queue_client_example", err)
		// 	return
		// }

		ds, err := ch.Consume(
			"dlx_queue_client_example",
			"",
			true,  // auto-ack for simplicity of example
			false, // exclusive
			false, // no-local
			false, // nowait
			nil,
		)
		if err != nil {
			fmt.Printf("Error consuming from dlx queue: %v\n", err)
			return
		}

		for d := range ds {
			fmt.Printf("DLX received message %q\n", string(d.Body))
		}
		// TODO close cleanly on ctx.Done()
	}()

	<-ctx.Done()
	fmt.Println("Context is done, stop consuming")

	return nil
}

func randNack() bool {
	return rand.Float32() < 0.5
}
