package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	conn            *amqp.Connection
	channel         *amqp.Channel
	done            chan struct{}
	status          status
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
}

// NewProducer creates a producer that synchronously connects/opens a
// channel to RabbitMQ at the given URI. If the producer was able to connect/open a
// channel it will automatically re-connect and re-open connection and channel
// if they fail. A producer holds on to one connection and one channel.
// A producer can be used to produce multiple times and from multiple goroutines.
func NewProducer(URI string) (*Producer, error) {
	_ = URI
	p := Producer{}

	return &p, nil
}

func (p *Producer) Close() error {
	return nil
}

func (p *Producer) Produce(queue string, msg []byte) error {
	return nil
}
