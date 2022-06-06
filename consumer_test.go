package rabbitmq_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/client"
	"github.com/dhis2-sre/rabbitmq"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (s *consumerSuite) TestValidateConsumerOptions() {
	assert := s.Assert()
	require := s.Require()

	id := uuid.NewString()
	maxCnTag := 256
	maxPrefix := maxCnTag - len(id) - 1 // 1 as we separate prefix and uuid using "-"

	consumer, err := rabbitmq.NewConsumer(s.rabbitURI, rabbitmq.WithConsumerPrefix(strings.Repeat("a", maxPrefix)))
	assert.NoError(err, fmt.Sprintf("consumer prefix of length %d should be valid", maxPrefix))
	if err == nil {
		require.NoError(consumer.Close())
	}

	consumer, err = rabbitmq.NewConsumer(s.rabbitURI, rabbitmq.WithConsumerPrefix(strings.Repeat("a", maxPrefix+1)))
	assert.ErrorContainsf(err, fmt.Sprintf("consumer prefix exceeded max length of %d", maxPrefix), "consumer prefix of length %d should not be valid", maxPrefix+1)
	if err == nil {
		require.NoError(consumer.Close())
	}
}

func (s *consumerSuite) TestNewConsumerFailsToConnect() {
	assert := s.Assert()

	consumer, err := rabbitmq.NewConsumer("amqp://wronguser:wrongpw@localhost:5672")

	assert.Error(err)

	// in case we do not return an error, make sure to close the consumer
	if err == nil {
		assert.NoError(consumer.Close())
	}
}

type temporary interface {
	Temporary() bool
}

func (s *consumerSuite) TestConsumerFailsDueToClosedConnection() {
	require := s.Require()
	assert := s.Assert()

	consumer, err := rabbitmq.NewConsumer(s.rabbitURI)
	require.NoError(err)
	require.NoError(consumer.Close())

	_, err = consumer.Consume("test_queue", func(_ amqp.Delivery) {
		require.Fail("should not deliver message")
	})
	// closed connections are not considered temporary errors
	// retrying the same Consume() would never succeed
	var temp temporary
	assert.Error(err)
	assert.False(errors.As(err, &temp))
}

func (s *consumerSuite) TestConsumerFailsDueToDifferingQueueProperties() {
	require := s.Require()
	assert := s.Assert()

	consumer, err := rabbitmq.NewConsumer(s.rabbitURI)
	require.NoError(err)
	defer func() { require.NoError(consumer.Close()) }()

	// provoke an error during Consume() by
	// declaring the same queue with different properties than what we do
	// within Consume()
	queue := "test_consume"
	_, err = s.amqpClient.ch.QueueDeclare(
		queue,
		true,  // Durable
		true,  // Delete when unused
		true,  // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	require.NoError(err)

	_, err = consumer.Consume(queue, func(_ amqp.Delivery) {
		require.Fail("should not deliver message")
	})

	// declaring a queue with differing properties are not considered temporary errors
	// retrying the same Consume() would never succeed
	var temp temporary
	assert.Error(err)
	assert.False(errors.As(err, &temp))
}

func (s *consumerSuite) TestConsume() {
	require := s.Require()
	assert := s.Assert()

	consumer, err := rabbitmq.NewConsumer(s.rabbitURI)
	require.NoError(err)
	defer func() { require.NoError(consumer.Close()) }()

	queue1 := "test_queue_1"
	msg1 := make(chan string)
	_, err = consumer.Consume(queue1, func(d amqp.Delivery) {
		msg1 <- string(d.Body)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)

	queue2 := "test_queue_2"
	msg2 := make(chan string)
	_, err = consumer.Consume(queue2, func(d amqp.Delivery) {
		msg2 <- string(d.Body)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)

	err = s.amqpClient.ch.Publish("", queue1, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("msg1"),
	})
	require.NoError(err)

	err = s.amqpClient.ch.Publish("", queue2, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("msg2"),
	})
	require.NoError(err)

	fmt.Println("Waiting on message consumption...")
	tm := time.NewTimer(s.timeout)
	defer tm.Stop()
	var got1, got2 string
	for n := 2; n > 0; {
		select {
		case <-tm.C:
			assert.Fail("Timed out waiting on message consumption")
			n = 0
		case got1 = <-msg1:
			n--
		case got2 = <-msg2:
			n--
		}
	}
	assert.Equal("msg1", got1)
	assert.Equal("msg2", got2)
}

func (s *consumerSuite) TestReconnectConsumerConnection() {
	require := s.Require()
	assert := s.Assert()

	proxyRabbitPort := "26379"
	proxyRabbitPortFull := proxyRabbitPort + "/tcp"
	proxyContainer, err := NewToxiProxy(s.ctx, s.networkName, WithPorts(proxyRabbitPortFull))
	require.NoError(err)
	defer func() { require.NoError(proxyContainer.Terminate(s.ctx)) }()

	toxi := toxiproxy.NewClient(proxyContainer.apiURI)
	proxy, err := toxi.CreateProxy("test_rabbitmq", "[::]:"+proxyRabbitPort, s.rabbitC.networkAlias+":"+s.rabbitC.port)
	require.NoError(err)

	rabbitExposedPort, ok := proxyContainer.ports[proxyRabbitPortFull]
	require.True(ok, "unable to get exposed RabbitMQ port")
	uri := fmt.Sprintf("amqp://%s:%s@%s:%s", s.rabbitC.usr, s.rabbitC.pw, "127.0.0.1", rabbitExposedPort)
	consumer, err := rabbitmq.NewConsumer(uri)
	require.NoError(err)
	defer func() { require.NoError(consumer.Close()) }()

	fmt.Println("Drop the consumers connection to RabbitMQ before calling Consume()")
	require.NoError(proxy.Disable())

	queue := "test_queue"
	// account for a delay in the consumer settings its status to
	// reconnnecting after the proxy dropped the connection.
	require.Eventually(func() bool {
		_, err = consumer.Consume(queue, func(d amqp.Delivery) {})
		return err != nil
	}, s.timeout, time.Second)
	require.Error(err)
	// consumer re-connecting is a temporary error
	// retrying the same Consume() could succeed at some point
	var temp temporary
	assert.Error(err)
	require.True(errors.As(err, &temp), "consumer re-connecting should be a temporary error")

	require.NoError(proxy.Enable())

	msg := make(chan string)
	require.Eventually(func() bool {
		_, err := consumer.Consume(queue, func(d amqp.Delivery) {
			msg <- string(d.Body)
			require.NoError(d.Ack(false))
		})

		return err == nil
	}, s.timeout, time.Second)

	// cannot publish before Consume() as queue is not persistent (yet ;) )
	err = s.amqpClient.ch.Publish("", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("foo"),
	})
	require.NoError(err)

	fmt.Println("Waiting on message consumption...")
	select {
	case <-time.After(s.timeout):
		require.FailNow("Timed out waiting on message consumption")
	case got := <-msg:
		require.Equal("foo", got)
	}
}

func (s *consumerSuite) TestCancellation() {
	require := s.Require()
	assert := s.Assert()

	consumer, err := rabbitmq.NewConsumer(s.rabbitURI, rabbitmq.WithConsumerPrefix("myservice"))
	require.NoError(err)
	defer func() { require.NoError(consumer.Close()) }()

	queue := "test_queue_1"
	msg := make(chan string)
	var cnt int32
	cn, err := consumer.Consume(queue, func(d amqp.Delivery) {
		msg <- string(d.Body)
		atomic.AddInt32(&cnt, 1)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)
	require.True(strings.HasPrefix(cn, "myservice"),
		fmt.Sprintf("expected consumer tag with prefix %q, got %q", "myservice", cn))

	// message before cancellation should be received
	err = s.amqpClient.ch.Publish("", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("msg1"),
	})
	require.NoError(err)

	fmt.Println("Waiting on message consumption...")
	tm := time.NewTimer(s.timeout)
	defer tm.Stop()
	var got string
	select {
	case <-tm.C:
		assert.Fail("Timed out waiting on message consumption")
	case got = <-msg:
	}
	assert.Equal("msg1", got)

	fmt.Println("Cancelling consumption")
	// should be safe to call it multiple times
	err = consumer.Cancel(cn)
	require.NoError(err)
	err = consumer.Cancel(cn)
	require.NoError(err)

	// message after cancellation should NOT be received
	err = s.amqpClient.ch.Publish("", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("msg2"),
	})
	require.NoError(err)

	assert.Equal(int32(1), cnt)
}

func (s *consumerSuite) TestClose() {
	require := s.Require()

	consumer, err := rabbitmq.NewConsumer(s.rabbitURI)
	require.NoError(err)

	gr := 4
	errChan := make(chan error)
	done := make(chan struct{})
	for i := 0; i < gr; i++ {
		go func(c *rabbitmq.Consumer, errChan chan error, done chan struct{}) {
			err := c.Close()

			select {
			case errChan <- err:
				return
			case <-done:
				return
			}
		}(consumer, errChan, done)
	}

	tm := time.NewTimer(s.timeout)
	defer tm.Stop()
	for gr > 0 {
		select {
		case <-tm.C:
			close(done)
			require.FailNow("Timed out waiting on goroutines to report Close() result")
			return
		case err := <-errChan:
			require.NoError(err)
			gr--
		}
	}
}

type consumerSuite struct {
	suite.Suite
	ctx         context.Context
	network     testcontainers.Network
	networkName string
	rabbitC     *rabbitmqContainer
	rabbitURI   string
	amqpClient  *amqpTestClient
	timeout     time.Duration
}

func TestSuiteConsumerIntegration(t *testing.T) {
	suite.Run(t, new(consumerSuite))
}

func (s *consumerSuite) SetupSuite() {
	ctx := context.TODO()

	name := "test_rabbitmq-" + uuid.NewString()
	net, err := setupNetwork(ctx, name)
	s.Require().NoError(err, "failed setting up Docker network")
	s.network = net
	s.networkName = name
	s.timeout = time.Second * 30
}

func (s *consumerSuite) TearDownSuite() {
	ctx := context.TODO()

	err := s.network.Remove(ctx)
	s.Require().NoError(err, "failed tearing down Docker network")
}

func (s *consumerSuite) SetupTest() {
	require := s.Require()

	ctx := context.Background()
	rbc, err := NewRabbitMQ(ctx,
		WithNetwork(s.networkName, "rabbitmq"),
	)
	require.NoError(err, "failed setting up RabbitMQ")

	ac, err := setupAMQPTestClient(rbc.amqpURI)
	require.NoError(err, "failed setting up AMQP client")

	s.ctx = ctx
	s.rabbitC = rbc
	s.rabbitURI = rbc.amqpURI
	s.amqpClient = ac
}

func (s *consumerSuite) TearDownTest() {
	require := s.Require()

	require.NoError(s.amqpClient.conn.Close())
	require.NoError(s.rabbitC.Terminate(s.ctx))
}
