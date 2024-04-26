package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/dhis2-sre/rabbitmq-client/pkg/rabbitmq"
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
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	consumer, err := rabbitmq.NewConsumer(addr,
		rabbitmq.WithConnectionName("rabbitmq_client_example_consumer"),
		rabbitmq.WithConsumerTagPrefix("rabbitmq_client_example_consumer"),
		rabbitmq.WithLogger(logger.WithGroup("rabbitmq")),
	)
	if err != nil {
		return err
	}
	logger.Info("Connected, login to management console at http://localhost:15672/ using user/pw guest.")
	defer func() {
		logger.Info("Closing consumer.")
		consumer.Close()
	}()

	for i := 0; i < 3; i++ {
		logger.Info("Trying to consume from queue.", "queue", queue, "address", addr)
		consumerTag, err := consumer.Consume(queue, func(d amqp.Delivery) {
			logger.Info("Received message.", "message", string(d.Body))
			_ = d.Ack(false)
		})
		if err == nil {
			logger.Info("Consume succeeded.", "consumerTag", consumerTag)
			break
		}

		var terr temporary
		if !errors.As(err, &terr) {
			return fmt.Errorf("error is not temporary: %w", err)
		}

		logger.Error("Consume failed.", "error", err)
		time.Sleep(time.Second * 5)
	}

	<-ctx.Done()
	logger.Info("Context is done, stop consuming.")

	return nil
}
