package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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

func (p *Producer) Produce(channel Channel, payload any) error {
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return fmt.Errorf("dial amqp connection: %v", err)
	}

	defer func(conn *amqp.Connection) {
		err := conn.Close()
		if err != nil {
			p.logger.Error("close connection", "error", err.Error())
		}
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("create channel: %v", err)
	}

	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {
			p.logger.Error("close channel", "error", err.Error())
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
	if err != nil {
		return fmt.Errorf("declare queue: %v", err)
	}

	marshal, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %v", err)
	}

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
	if err != nil {
		return fmt.Errorf("publish message: %v", err)
	}

	p.logger.Debug("message produced", "channel", channel)

	return nil
}
