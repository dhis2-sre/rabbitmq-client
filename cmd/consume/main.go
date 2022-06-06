package main

import (
	"context"
	"errors"
	"fmt"
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
		ct, err := consumer.Consume(queue, func(d amqp.Delivery) {
			fmt.Printf("received: %s\n", string(d.Body))
			_ = d.Ack(false)
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

	<-ctx.Done()
	fmt.Println("Context is done, stop consuming")

	return nil
}
