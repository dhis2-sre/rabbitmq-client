package rabbitmq

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DefaultReconnectWait     = 5 * time.Second
	DefaultReopenChannelWait = 2 * time.Second
	maxConsumerTagPrefix     = 219 // 256 (max AMQP-0-9-1 consumer tag) - 36 (UUID) - 1 ("-" prefix separator)
)

// An Options customizes the consumer.
type Options struct {
	// ConsumerPrefix sets the prefix to the auto-generated consumer tag. This
	// can aid in observing/debugging consumers on a channel (RabbitMQ management).
	ConsumerPrefix string

	// ReconnectWait sets the duration to wait after a failed attempt to connect to
	// RabbitMQ.
	ReconnectWait time.Duration

	// ReopenChannelWait sets the duration to wait after a failed attempt to open a
	// channel on a RabbitMQ connection.
	ReopenChannelWait time.Duration
}

// An Option configures consumer options.
type Option func(*Options)

// WithConsumerPrefix sets the prefix to the auto-generated consumer tag. The
// consumer tag is returned by Consume(). This can aid in observing/debugging
// consumers on a channel (RabbitMQ management/CLI).
func WithConsumerPrefix(prefix string) Option {
	return func(o *Options) {
		// "-" to separate it from the UUID
		o.ConsumerPrefix = prefix + "-"
	}
}

// WithReconnectWait sets the duration to wait after a failed attempt to connect to
// RabbitMQ.
func WithReconnectWait(wait time.Duration) Option {
	return func(o *Options) {
		o.ReconnectWait = wait
	}
}

// WithReopenChannelWait sets the duration to wait after a failed attempt to open
// a channel on a RabbitMQ connection.
func WithReopenChannelWait(wait time.Duration) Option {
	return func(o *Options) {
		o.ReopenChannelWait = wait
	}
}

type Consumer struct {
	mu sync.RWMutex

	opts   *Options
	logger *log.Logger

	connector *connecter
}

// NewConsumer creates a Consumer that synchronously connects/opens a
// channel to RabbitMQ at the given URI. If the consumer was able to connect/open a
// channel it will automatically re-connect and re-open connection and channel
// if they fail. A consumer holds on to one connection and one channel.
// A consumer can be used to consume multiple times and from multiple goroutines.
func NewConsumer(URI string, options ...Option) (*Consumer, error) {
	opts := &Options{
		ReconnectWait:     DefaultReconnectWait,
		ReopenChannelWait: DefaultReopenChannelWait,
	}
	for _, o := range options {
		o(opts)
	}
	err := validateOptions(opts)
	if err != nil {
		return nil, err
	}

	cn, err := newConnecter(URI)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		opts:      opts,
		logger:    log.New(os.Stdout, "", log.LstdFlags),
		connector: cn,
	}, nil
}

func validateOptions(opts *Options) error {
	if len(opts.ConsumerPrefix)-1 > maxConsumerTagPrefix {
		return fmt.Errorf("consumer prefix exceeded max length of %d", maxConsumerTagPrefix)
	}

	return nil
}

type tempError struct {
	err string
}

func (te tempError) Error() string {
	return te.err
}

func (te tempError) Temporary() bool {
	return true
}

// Consume registers the consumer to receive messages from given queue.
// Consume synchronously declares and registers a consumer to the queue.
// Once registered it will return the consumer tag and nil error.
// receive will be called for every message. Pass the consumer tag to
// Cancel() to stop consuming messages. Consume will not re-consume if the
// connection or channel close even if they only close temporarily.
// Consume can be called multiple times and from multiple goroutines.
func (c *Consumer) Consume(queue string, receive func(d amqp.Delivery)) (string, error) {
	c.mu.RLock()

	if c.connector.status != connected {
		status := c.connector.status
		c.mu.RUnlock()
		if status == reconnecting {
			return "", tempError{err: "temporarily failed to consume: re-connecting with broker"}
		}
		return "", fmt.Errorf("failed to consume: connection is in %q state", status)
	}

	_, err := c.connector.channel.QueueDeclare(
		queue,
		false, // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		c.mu.RUnlock()
		return "", err
	}
	id := c.opts.ConsumerPrefix + uuid.NewString()
	ds, err := c.connector.channel.Consume(
		queue,
		id,    // Consumer
		false, // Auto-Ack
		false, // Exclusive
		false, // No-local
		false, // No-Wait
		nil,   // Args
	)
	if err != nil {
		c.mu.RUnlock()
		return "", err
	}
	c.mu.RUnlock()

	go func() {
		for d := range ds {
			receive(d)
		}
	}()
	return id, nil
}

// Cancel consuming messages for given consumer. The consumer identifier is
// returned by Consume().
// It is safe to call this method multiple times and in multiple goroutines.
func (c *Consumer) Cancel(consumer string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.connector.status != connected {
		status := c.connector.status
		if status == reconnecting {
			return tempError{err: "temporarily failed to cancel: re-connecting with broker"}
		}
		return fmt.Errorf("failed to cancel: connection is in %q state", status)
	}

	return c.connector.channel.Cancel(consumer, false)
}

// Close connection and channel. A new consumer needs to be
// created in order to consume again after closing it.
// It is safe to call this method multiple times and in multiple goroutines.
func (c *Consumer) Close() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.connector.close()
}
