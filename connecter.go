package rabbitmq

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

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

type connecter struct {
	mu sync.RWMutex

	opts   *Options
	logger *log.Logger

	conn            *amqp.Connection
	channel         *amqp.Channel
	done            chan struct{}
	status          status
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
}

// NewConsumer creates a Consumer that synchronously connects/opens a
// channel to RabbitMQ at the given URI. If the consumer was able to connect/open a
// channel it will automatically re-connect and re-open connection and channel
// if they fail. A consumer holds on to one connection and one channel.
// A consumer can be used to consume multiple times and from multiple goroutines.
func newConnecter(URI string, options ...Option) (*connecter, error) {
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

	c := connecter{
		opts:   opts,
		logger: log.New(os.Stdout, "", log.LstdFlags),
		done:   make(chan struct{}),
		status: disconnected,
	}

	err = c.connect(URI)
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

// connect will create a new AMQP connection
func (c *connecter) connect(addr string) error {
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

	c.notifyConnClose = make(chan *amqp.Error)
	c.conn.NotifyClose(c.notifyConnClose)

	return nil
}

// createChannel will open channel. Assumes a connection is open.
func (c *connecter) createChannel() error {
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

// maintainConnection ensure the consumers AMQP connection and channel are both
// open. re-connecting on notifyConnClose events,
// re-opening a channel on notifyChanClose events
func (c *connecter) maintainConnection(addr string) {
	select {
	case <-c.done:
		c.logger.Println("Stopping connection loop due to done closed")
		return
	case <-c.notifyConnClose:
		c.logger.Println("Connection closed. Re-connecting...")

		for {
			err := c.connect(addr)
			if err != nil {
				c.logger.Println("Failed to connect. Retrying...")
				t := time.NewTimer(c.opts.ReconnectWait)
				select {
				case <-c.done:
					if !t.Stop() {
						<-t.C
					}
					c.logger.Println("Stopping connection loop due to done closed")
					return
				case <-t.C:
				}
				continue
			}
			c.logger.Println("Consumer connection re-established")

			c.openChannel()
			c.logger.Println("Consumer connection and channel re-established")
			break
		}
	case <-c.notifyChanClose:
		c.logger.Println("Channel closed. Re-opening new one...")
		c.openChannel()
		c.logger.Println("Consumer channel re-established")
	}

	c.maintainConnection(addr)
}

// openChannel opens a channel. Assumes a connection is open.
func (c *connecter) openChannel() {
	for {
		err := c.createChannel()
		if err == nil {
			return
		}

		c.logger.Println("Failed to open channel. Retrying...")
		t := time.NewTimer(c.opts.ReopenChannelWait)
		select {
		case <-c.done:
			if !t.Stop() {
				<-t.C
			}
			return
		case <-t.C:
		}
	}
}

// Close connection and channel. A new consumer needs to be
// created in order to consume again after closing it.
// It is safe to call this method multiple times and in multiple goroutines.
func (c *connecter) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status != closed {
		c.status = closed
		// stop re-connecting/re-opening a channel
		close(c.done)
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
