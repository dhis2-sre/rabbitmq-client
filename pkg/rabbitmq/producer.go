package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Channel string

func ProvideProducer(logger *slog.Logger, url string) Producer {
	return Producer{logger, url}
}

type Producer struct {
	logger *slog.Logger
	url    string
}

func (p *Producer) Produce(channel Channel, payload any) {
	p.logger.Info("Channel: %s", channel)
	p.logger.Info("Payload: %+v", payload)

	conn, err := amqp.Dial(p.url)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer func(conn *amqp.Connection) {
		err := conn.Close()
		if err != nil {
			failOnError(err, "Failed to close connection")
		}
	}(conn)

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {
			failOnError(err, "Failed to close channel")
		}
	}(ch)

	q, err := ch.QueueDeclare(
		string(channel),
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	marshal, err := json.Marshal(payload)
	failOnError(err, "Failed to json serialize payload")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	publishing := amqp.Publishing{
		ContentType: "application/json",
		Body:        marshal,
	}
	err = ch.PublishWithContext(ctx,
		"",
		q.Name,
		false,
		false,
		publishing)
	failOnError(err, "Failed to publish a message")

	p.logger.Info("[%s] Sent %+v", channel, payload)
}

func failOnError(err error, msg string) {
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s: %s", msg, err) // nolint:errcheck
		os.Exit(1)
	}
}
