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

type status int

func (s status) String() string {
	switch s {
	case disconnected:
		return "disconnected"
	case connecting:
		return "connecting"
	case connected:
		return "connected"
	case reconnecting:
		return "reconnecting"
	case closed:
		return "closed"
	}
	return "unknown"
}

const (
	disconnected = status(iota)
	connecting
	connected
	reconnecting
	closed
)

type Consumer struct {
	mu sync.RWMutex

	opts   *Options
	logger *log.Logger

	conn            *amqp.Connection
	channel         *amqp.Channel
	done            chan struct{}
	status          status
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error

	consumers map[string]func(c *Consumer) error // consumers tracks the registered receiver functions per consumer identity
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

	c := Consumer{
		opts:      opts,
		logger:    log.New(os.Stdout, "", log.LstdFlags),
		done:      make(chan struct{}),
		status:    disconnected,
		consumers: make(map[string]func(*Consumer) error),
	}

	err = c.createConnection(URI)
	if err != nil {
		return nil, err
	}
	err = c.createChannel()
	if err != nil {
		return nil, err
	}

	go c.maintainConnection(URI)

	return &c, nil
}

func validateOptions(opts *Options) error {
	if len(opts.ConsumerPrefix)-1 > maxConsumerTagPrefix {
		return fmt.Errorf("consumer prefix exceeded max length of %d", maxConsumerTagPrefix)
	}

	return nil
}

// createConnection will create a new AMQP connection
func (c *Consumer) createConnection(addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status == connected || c.status == reconnecting {
		c.status = reconnecting
	} else {
		c.status = connecting
	}

	conn, err := amqp.Dial(addr)
	if err != nil {
		return err
	}
	c.conn = conn

	c.notifyConnClose = make(chan *amqp.Error, 1)
	c.conn.NotifyClose(c.notifyConnClose)

	return nil
}

// createChannel will open channel. Assumes a connection is open.
func (c *Consumer) createChannel() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status == connected || c.status == reconnecting {
		c.status = reconnecting
	} else {
		c.status = connecting
	}

	ch, err := c.conn.Channel()
	if err != nil {
		return err
	}
	c.channel = ch

	c.notifyChanClose = make(chan *amqp.Error, 1)
	c.channel.NotifyClose(c.notifyChanClose)
	c.status = connected

	return nil
}

// maintainConnection ensures the consumers AMQP connection and channel are both open. re-connecting
// on notifyConnClose events, re-opening a channel on notifyChanClose events
func (c *Consumer) maintainConnection(addr string) {
	select {
	case <-c.done:
		c.logger.Println("Done closed. Stop trying to maintain the connection.")
		return
	case <-c.notifyConnClose:
		c.logger.Println("Connection closed. Opening a new connection...")

		for {
			err := c.createConnection(addr)
			if err != nil {
				c.logger.Println("Failed to connect. Retrying...")
				t := time.NewTimer(c.opts.ReconnectWait)
				select {
				case <-c.done:
					if !t.Stop() {
						<-t.C
					}
					c.logger.Println("Done closed. Stop trying to open a connection.")
					return
				case <-t.C:
				}
				continue
			}
			c.logger.Println("Consumer connection re-established. Opening a new channel...")
			c.openChannel()
			break
		}
	case <-c.notifyChanClose:
		c.logger.Println("Channel closed. Opening a new channel...")
		c.openChannel()
	}

	c.maintainConnection(addr)
}

// openChannel opens a channel. Assumes a connection is open.
func (c *Consumer) openChannel() {
	for {
		err := c.createChannel()
		if err == nil {
			c.logger.Println("Consumer channel re-established. Registering consumers...")
			c.registerConsumers()
			return
		}

		c.logger.Println("Failed to open channel. Retrying...")
		t := time.NewTimer(c.opts.ReopenChannelWait)
		select {
		case <-c.done:
			if !t.Stop() {
				<-t.C
			}
			c.logger.Println("Done closed. Stop trying to open a channel.")
			return
		case <-c.notifyConnClose:
			c.logger.Println("Connection closed. Stop trying to open a channel.")
			return
		case <-t.C:
		}
	}
}

func (c *Consumer) registerConsumers() {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for consumerID, consumeFunc := range c.consumers {
		// We need to exit and release our lock if the connection or channel got closed otherwise we
		// won't be able to acquire a write lock to set the new connection and channel
		select {
		case <-c.done:
			c.logger.Println("Done closed. Stop trying to register consumers.")
			return
		case <-c.notifyConnClose:
			c.logger.Println("Connection closed. Stop trying to register consumers.")
			return
		case <-c.notifyChanClose:
			c.logger.Println("Channel closed. Stop trying to register consumers.")
			return
		default:
			c.logger.Printf("Registering consumer %q", consumerID)
			// We assume for now that err!=nil means either the connection or channel is closed. In
			// these cases retrying to consume would never succeed as we are holding the lock
			// preventing the connection/channel to be re-established. So we log and move on to
			// check the notification channels and exit to our connection maintenance logic.
			err := consumeFunc(c)
			if err != nil {
				c.logger.Printf("Failed to register consumer %q due to: %v\n", consumerID, err)
			}
		}
	}
	c.logger.Println("Done registering consumers")
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

// Consume registers the consumer to receive messages from given queue. Consume synchronously
// declares and registers a consumer to the queue. Once registered it will return the consumer
// identifier and nil error. receive will be called for every message. Pass the consumer identifier
// to Cancel() to stop consuming messages. Consume will re-consume if the connection or channel
// close only if it returned successfully at first. Consume can be called multiple times and from
// multiple goroutines.
func (c *Consumer) Consume(queue string, receive func(d amqp.Delivery)) (string, error) {
	c.mu.RLock()

	if c.status != connected {
		status := c.status
		c.mu.RUnlock()
		if status == reconnecting {
			return "", tempError{err: "temporarily failed to consume: re-connecting with broker"}
		}
		return "", fmt.Errorf("failed to consume: connection is in %q state", status)
	}

	_, err := c.channel.QueueDeclare(
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

	consumerID := c.opts.ConsumerPrefix + uuid.NewString()
	deliveries, err := c.channel.Consume(
		queue,
		consumerID,
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

	c.mu.Lock()
	c.consumers[consumerID] = consumeFunc(queue, consumerID, receive)
	c.mu.Unlock()

	go func() {
		for d := range deliveries {
			receive(d)
		}
	}()

	return consumerID, nil
}

func consumeFunc(queue, consumerID string, receive func(delivery amqp.Delivery)) func(c *Consumer) error {
	return func(c *Consumer) error {
		c.mu.RLock()

		deliveries, err := c.channel.Consume(
			queue,
			consumerID,
			false, // Auto-Ack
			false, // Exclusive
			false, // No-local
			false, // No-Wait
			nil,   // Args
		)
		if err != nil {
			c.mu.RUnlock()
			return fmt.Errorf("failed to consume: %v", err)
		}
		c.mu.RUnlock()

		go func() {
			for d := range deliveries {
				receive(d)
			}
		}()
		return nil
	}
}

// Cancel consuming messages for given consumer. The consumer identifier is
// returned by Consume().
// It is safe to call this method multiple times and in multiple goroutines.
func (c *Consumer) Cancel(consumer string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status != connected {
		status := c.status
		if status == reconnecting {
			return tempError{err: "temporarily failed to cancel: re-connecting with broker"}
		}
		return fmt.Errorf("failed to cancel: connection is in %q state", status)
	}

	err := c.channel.Cancel(consumer, false)
	// We assume for now that Cancel() = err!=nil means the consumer stopped consuming messages. For
	// example Cancel() on a closed channel or connection will fail. This is why we would want to
	// remove the consumer and not re-register it once connection/channel are back up.
	delete(c.consumers, consumer)
	return err
}

// Close connection and channel. A new consumer needs to be
// created in order to consume again after closing it.
// It is safe to call this method multiple times and in multiple goroutines.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status != closed {
		c.status = closed
		// stop re-connecting/re-opening a channel
		close(c.done)
		clear(c.consumers)
	}

	// nothing to close if we do not have an open connection and channel
	var errCh error
	if c.channel != nil && !c.channel.IsClosed() {
		errCh = c.channel.Close()
		if errCh != nil {
			errCh = fmt.Errorf("failed to close channel: %w", errCh)
		}
	}
	var errCon error
	if c.conn != nil && !c.conn.IsClosed() {
		errCon = c.conn.Close()
	}
	if errCon != nil {
		return fmt.Errorf("failed to close connection: %w", errCon)
	}

	return errCh
}
