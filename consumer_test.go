package rabbitmq_test

import (
	"context"

	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dhis2-sre/rabbitmq-client"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
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

	consumer, err := rabbitmq.NewConsumer(s.rabbitURI)
	require.NoError(err)
	defer func() { require.NoError(consumer.Close()) }()

	msg := make(chan string, 2)

	queue1 := "test_queue_1"
	_, err = consumer.Consume(queue1, func(d amqp.Delivery) {
		msg <- string(d.Body)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)

	queue2 := "test_queue_2"
	_, err = consumer.Consume(queue2, func(d amqp.Delivery) {
		msg <- string(d.Body)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)

	// nolint:staticcheck // SA1019 https://github.com/rabbitmq/amqp091-go/issues/195
	err = s.amqpClient.ch.Publish("", queue1, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("msg on queue 1"),
	})
	require.NoError(err)

	// nolint:staticcheck // SA1019 https://github.com/rabbitmq/amqp091-go/issues/195
	err = s.amqpClient.ch.Publish("", queue2, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("msg on queue 2"),
	})
	require.NoError(err)

	s.T().Log("Waiting on messages...")
	tm := time.NewTimer(s.timeout)
	defer tm.Stop()
	var got []string
	for len(got) < 2 {
		select {
		case <-tm.C:
			require.FailNowf("Timed out waiting on messages.", "Want 2 messages, got %v instead", got)
		case m := <-msg:
			got = append(got, m)
			s.T().Logf("Received message %q", m)
		}
	}

	require.ElementsMatch(got, []string{"msg on queue 1", "msg on queue 2"})
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

	s.T().Log("Drop and prevent connections to RabbitMQ before calling Consume()")
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

	s.T().Log("Allow connections to RabbitMQ to start the consumers re-connection logic")
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
	// nolint:staticcheck // SA1019 https://github.com/rabbitmq/amqp091-go/issues/195
	err = s.amqpClient.ch.Publish("", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("msg1"),
	})
	require.NoError(err)

	s.T().Log("Waiting on messages...")
	select {
	case <-time.After(s.timeout):
		require.FailNow("Timed out waiting on messages.")
	case got := <-msg:
		require.Equal("msg1", got)
		s.T().Logf("Received message %q", got)
	}
}

func (s *consumerSuite) TestReconnectConsume() {
	require := s.Require()

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

	msg := make(chan string, 2)

	queue1 := "test_queue_1"
	consumer1, err := consumer.Consume(queue1, func(d amqp.Delivery) {
		msg <- string(d.Body)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)
	defer func() { require.NoError(consumer.Cancel(consumer1)) }()

	queue2 := "test_queue_2"
	consumer2, err := consumer.Consume(queue2, func(d amqp.Delivery) {
		msg <- string(d.Body)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)
	defer func() { require.NoError(consumer.Cancel(consumer2)) }()

	s.T().Log("Drop and prevent connections to RabbitMQ after calling Consume() but before publishing messages.")
	require.NoError(proxy.Disable())
	s.T().Log("Allow connections to RabbitMQ to start the registration of consumers.")
	require.NoError(proxy.Enable())

	// cannot publish before Consume() as queue is not persistent (yet ;) )
	// nolint:staticcheck // SA1019 https://github.com/rabbitmq/amqp091-go/issues/195
	err = s.amqpClient.ch.Publish("", queue1, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("msg on queue 1"),
	})
	require.NoError(err)

	// nolint:staticcheck // SA1019 https://github.com/rabbitmq/amqp091-go/issues/195
	err = s.amqpClient.ch.Publish("", queue2, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("msg on queue 2"),
	})
	require.NoError(err)

	s.T().Log("Waiting on messages...")
	tm := time.NewTimer(s.timeout)
	defer tm.Stop()
	var got []string
	for len(got) < 2 {
		select {
		case <-tm.C:
			require.FailNowf("Timed out waiting on messages.", "Want 2 messages, got %v instead", got)
		case m := <-msg:
			got = append(got, m)
			s.T().Logf("Received message %q", m)
		}
	}

	require.ElementsMatch(got, []string{"msg on queue 1", "msg on queue 2"})
}

func (s *consumerSuite) TestCancellation() {
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
	consumer, err := rabbitmq.NewConsumer(uri, rabbitmq.WithConsumerPrefix("myservice"))
	require.NoError(err)
	defer func() { require.NoError(consumer.Close()) }()

	msg := make(chan string)

	queue := "test_queue_1"
	var messageCountConsumer1 int32
	consumer1, err := consumer.Consume(queue, func(d amqp.Delivery) {
		msg <- string(d.Body)
		atomic.AddInt32(&messageCountConsumer1, 1)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)

	require.True(strings.HasPrefix(consumer1, "myservice"),
		fmt.Sprintf("expected consumer tag with prefix %q, got %q", "myservice", consumer1))

	s.T().Log("Sending first message.")
	// message before cancellation should be received
	// nolint:staticcheck // SA1019 https://github.com/rabbitmq/amqp091-go/issues/195
	err = s.amqpClient.ch.Publish("", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("msg1"),
	})
	require.NoError(err)

	s.T().Log("Waiting on messages...")
	select {
	case <-time.After(s.timeout):
		require.FailNow("Timed out waiting on messages.")
	case got := <-msg:
		require.Equal("msg1", got)
		s.T().Logf("Received message %q", got)
	}

	s.T().Log("Registering a second consumer to the same queue.")
	consumer2, err := consumer.Consume(queue, func(d amqp.Delivery) {
		msg <- string(d.Body)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)
	defer func() { require.NoError(consumer.Cancel(consumer2)) }()

	s.T().Log("Cancelling consumer1")
	// should be safe to call it multiple times
	err = consumer.Cancel(consumer1)
	require.NoError(err)
	err = consumer.Cancel(consumer1)
	require.NoError(err)

	s.T().Log("Drop and prevent connections to RabbitMQ to show consumer1 will not be registered again.")
	require.NoError(proxy.Disable())
	s.T().Log("Allow connections to RabbitMQ to start the registration of consumers.")
	require.NoError(proxy.Enable())

	s.T().Log("Sending second message.")
	// message after cancellation should NOT be received by consumer1
	err = s.amqpClient.ch.Publish("", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("msg2"),
	})
	require.NoError(err)

	s.T().Log("Waiting on messages...")
	select {
	case <-time.After(s.timeout):
		require.FailNow("Timed out waiting on messages.")
	case got := <-msg:
		require.Equal("msg2", got)
		s.T().Logf("Received message %q", got)
	}

	assert.Equalf(int32(1), messageCountConsumer1, "First consumer got %d messages, want %d instead", messageCountConsumer1, 1)
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
	require := s.Require()
	ctx := context.TODO()

	require.NoError(s.network.Remove(ctx), "failed to remove the Docker network")
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

	require.NoError(s.amqpClient.conn.Close(), "failed to close the AMQP client connection")
	require.NoError(s.rabbitC.Terminate(s.ctx), "failed to terminate the RabbitMQ container")
}
