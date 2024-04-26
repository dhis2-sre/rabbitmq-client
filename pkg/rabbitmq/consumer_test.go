package rabbitmq_test

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dhis2-sre/rabbitmq-client/pkg/inttest"
	"github.com/dhis2-sre/rabbitmq-client/pkg/rabbitmq"
	"github.com/google/uuid"
	amqpgo "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// timeout waiting on messages or other queue operations
var timeout time.Duration = 30 * time.Second

func TestNewConsumer(t *testing.T) {
	amqp := inttest.SetupRabbitMQ(t)

	id := uuid.NewString()
	maxCnTag := 256
	maxPrefix := maxCnTag - len(id) - 1 // 1 as we separate prefix and uuid using "-"

	t.Run("ConsumerPrefixValid", func(t *testing.T) {
		consumer, err := rabbitmq.NewConsumer(amqp.ProxiedURI,
			rabbitmq.WithConsumerTagPrefix(strings.Repeat("a", maxPrefix)),
		)

		assert.NoError(t, err,
			fmt.Sprintf("consumer prefix of length %d should be valid", maxPrefix),
		)
		if err == nil {
			require.NoError(t, consumer.Close())
		}
	})

	t.Run("ConsumerPrefixInvalid", func(t *testing.T) {
		consumer, err := rabbitmq.NewConsumer(amqp.ProxiedURI,
			rabbitmq.WithConsumerTagPrefix(strings.Repeat("a", maxPrefix+1)),
		)

		assert.ErrorContainsf(t, err,
			fmt.Sprintf("consumer prefix exceeded max length of %d", maxPrefix),
			"consumer prefix of length %d should not be valid", maxPrefix+1)
		if err == nil {
			require.NoError(t, consumer.Close())
		}
	})

	t.Run("FailsToConnectDueToWrongCredentials", func(t *testing.T) {
		consumer, err := rabbitmq.NewConsumer(amqp.GetProxiedURI("wrongUser", "wrongPassword"))

		assert.Error(t, err)

		// in case we do not return an error, make sure to close the consumer
		if err == nil {
			assert.NoError(t, consumer.Close())
		}
	})
}

func TestConsumeFailsDueToClosedConnection(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	amqp := inttest.SetupRabbitMQ(t)

	consumer, err := rabbitmq.NewConsumer(amqp.ProxiedURI,
		rabbitmq.WithConnectionName(t.Name()),
		rabbitmq.WithConsumerTagPrefix(t.Name()),
		rabbitmq.WithLogger(slog.New(slog.NewTextHandler(os.Stdout, nil))),
	)
	require.NoError(err)

	// closing the connection before call to Consume()
	require.NoError(consumer.Close())

	_, err = consumer.Consume("test_queue_1", func(_ amqpgo.Delivery) {
		require.Fail("should not deliver message")
	})
	// closed connections are not considered temporary errors retrying the same Consume() would
	// never succeed
	var temp temporary
	require.Error(err)
	assert.False(errors.As(err, &temp))
}

func TestConsumeFailsDueToDifferingQueueProperties(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	amqp := inttest.SetupRabbitMQ(t)

	consumer, err := rabbitmq.NewConsumer(amqp.ProxiedURI,
		rabbitmq.WithConnectionName(t.Name()),
		rabbitmq.WithConsumerTagPrefix(t.Name()),
		rabbitmq.WithLogger(slog.New(slog.NewTextHandler(os.Stdout, nil))),
	)
	require.NoError(err)
	defer func() { require.NoError(consumer.Close()) }()

	// provoke an error during Consume() by declaring the same queue with different properties than
	// what we do within Consume()
	queue := "test_queue_1"
	_, err = amqp.Channel.QueueDeclare(
		queue,
		true,  // Durable
		true,  // Delete when unused
		true,  // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	require.NoError(err)

	_, err = consumer.Consume(queue, func(_ amqpgo.Delivery) {
		require.Fail("should not deliver message")
	})

	// declaring a queue with differing properties are not considered temporary errors retrying the
	// same Consume() would never succeed
	var temp temporary
	require.Error(err)
	assert.False(errors.As(err, &temp))
}

func TestConsume(t *testing.T) {
	require := require.New(t)

	amqp := inttest.SetupRabbitMQ(t)

	consumer, err := rabbitmq.NewConsumer(amqp.ProxiedURI,
		rabbitmq.WithConnectionName(t.Name()),
		rabbitmq.WithConsumerTagPrefix(t.Name()),
		rabbitmq.WithLogger(slog.New(slog.NewTextHandler(os.Stdout, nil))),
	)
	require.NoError(err)
	defer func() { require.NoError(consumer.Close()) }()

	msg := make(chan string, 2)

	queue1 := "test_queue_1"
	_, err = consumer.Consume(queue1, func(d amqpgo.Delivery) {
		msg <- string(d.Body)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)

	queue2 := "test_queue_2"
	_, err = consumer.Consume(queue2, func(d amqpgo.Delivery) {
		msg <- string(d.Body)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)

	amqp.Publish(t, queue1, "msg on queue 1")
	amqp.Publish(t, queue2, "msg on queue 2")

	t.Log("Waiting on messages...")
	tm := time.NewTimer(timeout)
	defer tm.Stop()
	var got []string
	for len(got) < cap(msg) {
		select {
		case <-tm.C:
			require.FailNowf("Timed out waiting on messages.", "Want %d messages, got %v instead", cap(msg), got)
		case m := <-msg:
			got = append(got, m)
			t.Logf("Received message %q", m)
		}
	}

	require.ElementsMatch(got, []string{"msg on queue 1", "msg on queue 2"})
}

func TestConsumeReconnectsConnection(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	amqp := inttest.SetupRabbitMQ(t)

	consumer, err := rabbitmq.NewConsumer(amqp.ProxiedURI,
		rabbitmq.WithConnectionName(t.Name()),
		rabbitmq.WithConsumerTagPrefix(t.Name()),
		rabbitmq.WithLogger(slog.New(slog.NewTextHandler(os.Stdout, nil))),
	)
	require.NoError(err)
	defer func() { require.NoError(consumer.Close()) }()

	t.Log("Drop and prevent connections to RabbitMQ before calling Consume()")
	amqp.DisableConnections(t)

	queue := "test_queue_1"
	// account for a delay in the consumer settings its status to
	// reconnnecting after the proxy dropped the connection.
	require.Eventually(func() bool {
		_, err = consumer.Consume(queue, func(d amqpgo.Delivery) {})
		return err != nil
	}, timeout, time.Second)
	require.Error(err)
	// consumer re-connecting is a temporary error
	// retrying the same Consume() could succeed at some point
	var temp temporary
	assert.Error(err)
	require.True(errors.As(err, &temp), "consumer re-connecting should be a temporary error")

	t.Log("Allow connections to RabbitMQ to start the consumers re-connection logic")
	amqp.EnableConnections(t)

	msg := make(chan string)
	require.Eventually(func() bool {
		_, err := consumer.Consume(queue, func(d amqpgo.Delivery) {
			msg <- string(d.Body)
			require.NoError(d.Ack(false))
		})

		return err == nil
	}, timeout, time.Second)

	// cannot publish before Consume() as queue is not persistent (yet ;) )
	amqp.Publish(t, queue, "msg1")

	t.Log("Waiting on messages...")
	select {
	case <-time.After(timeout):
		require.FailNow("Timed out waiting on messages.")
	case got := <-msg:
		require.Equal("msg1", got)
		t.Logf("Received message %q", got)
	}
}

func TestConsumeRegistersConsumersAgain(t *testing.T) {
	// tests the case that consumers were connected and consuming messages and the connection
	// dropped and came back again
	require := require.New(t)

	amqp := inttest.SetupRabbitMQ(t)

	consumer, err := rabbitmq.NewConsumer(amqp.ProxiedURI,
		rabbitmq.WithConnectionName(t.Name()),
		rabbitmq.WithConsumerTagPrefix(t.Name()),
		rabbitmq.WithLogger(slog.New(slog.NewTextHandler(os.Stdout, nil))),
	)
	require.NoError(err)
	defer func() { require.NoError(consumer.Close()) }()

	msg := make(chan string, 2)

	queue1 := "test_queue_1"
	consumer1, err := consumer.Consume(queue1, func(d amqpgo.Delivery) {
		msg <- string(d.Body)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)
	defer func() { require.NoError(consumer.Cancel(consumer1)) }()

	queue2 := "test_queue_2"
	consumer2, err := consumer.Consume(queue2, func(d amqpgo.Delivery) {
		msg <- string(d.Body)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)
	defer func() { require.NoError(consumer.Cancel(consumer2)) }()

	t.Log("Drop and prevent connections to RabbitMQ after calling Consume() but before publishing messages.")
	amqp.DisableConnections(t)
	t.Log("Allow connections to RabbitMQ to start the registration of consumers.")
	amqp.EnableConnections(t)

	amqp.Publish(t, queue1, "msg on queue 1")
	amqp.Publish(t, queue2, "msg on queue 2")

	t.Log("Waiting on messages...")
	tm := time.NewTimer(timeout)
	defer tm.Stop()
	var got []string
	for len(got) < cap(msg) {
		select {
		case <-tm.C:
			require.FailNowf("Timed out waiting on messages.", "Want %d messages, got %v instead", cap(msg), got)
		case m := <-msg:
			got = append(got, m)
			t.Logf("Received message %q", m)
		}
	}

	require.ElementsMatch(got, []string{"msg on queue 1", "msg on queue 2"})
}

func TestConsumeRegistersConsumersAgainAndRedeclaresQueues(t *testing.T) {
	// non-durable queues are deleted if no one is bound to them or RabbitMQ restarts. In case a
	// consumer was connected and consuming messages and RabbitMQ goes down non-durable queues need
	// to be redeclared after reconnecting.
	require := require.New(t)

	amqp := inttest.SetupRabbitMQ(t)

	consumer, err := rabbitmq.NewConsumer(amqp.ProxiedURI,
		rabbitmq.WithConnectionName(t.Name()),
		rabbitmq.WithConsumerTagPrefix(t.Name()),
		rabbitmq.WithLogger(slog.New(slog.NewTextHandler(os.Stdout, nil))),
	)
	require.NoError(err)
	defer func() { require.NoError(consumer.Close()) }()

	msg := make(chan string)

	queue1 := "test_queue_1"
	consumer1, err := consumer.Consume(queue1, func(d amqpgo.Delivery) {
		msg <- string(d.Body)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)
	defer func() { require.NoError(consumer.Cancel(consumer1)) }()

	// cannot publish before Consume() as queue is not persistent (yet ;) )
	amqp.Publish(t, queue1, "msg1")

	t.Log("Waiting on messages...")
	select {
	case <-time.After(timeout):
		require.FailNow("Timed out waiting on messages.")
	case got := <-msg:
		require.Equal("msg1", got)
		t.Logf("Received message %q", got)
	}

	t.Log("Restart RabbitMQ to remove non-durable queues")
	amqp.Restart(t)

	// We have no insight into when the consumer is registered again. the consumer declares the
	// queue. So we might publish before the queue even exists. Therefore retry.
	done := make(chan struct{})
	defer close(done)
	amqp.PublishEvery(t, 100*time.Millisecond, done, queue1, "msg2")

	t.Log("Waiting on messages...")
	select {
	case <-time.After(timeout):
		require.FailNow("Timed out waiting on messages.")
	case got := <-msg:
		require.Equal("msg2", got)
		t.Logf("Received message %q", got)
	}
}

func TestCancel(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	amqp := inttest.SetupRabbitMQ(t)

	consumer, err := rabbitmq.NewConsumer(amqp.ProxiedURI,
		rabbitmq.WithConnectionName(t.Name()),
		rabbitmq.WithConsumerTagPrefix(t.Name()),
		rabbitmq.WithLogger(slog.New(slog.NewTextHandler(os.Stdout, nil))),
	)
	require.NoError(err)
	defer func() { require.NoError(consumer.Close()) }()

	msg := make(chan string)

	queue := "test_queue_1"
	var messageCountConsumer1 int32
	consumer1, err := consumer.Consume(queue, func(d amqpgo.Delivery) {
		msg <- string(d.Body)
		atomic.AddInt32(&messageCountConsumer1, 1)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)

	require.True(strings.HasPrefix(consumer1, t.Name()),
		fmt.Sprintf("expected consumer tag with prefix %q, got %q", t.Name(), consumer1))

	t.Log("Sending first message.")
	// message before cancellation should be received
	amqp.Publish(t, queue, "msg1")

	t.Log("Waiting on messages...")
	select {
	case <-time.After(timeout):
		require.FailNow("Timed out waiting on messages.")
	case got := <-msg:
		require.Equal("msg1", got)
		t.Logf("Received message %q", got)
	}

	t.Log("Registering a second consumer to the same queue.")
	consumer2, err := consumer.Consume(queue, func(d amqpgo.Delivery) {
		msg <- string(d.Body)
		require.NoError(d.Ack(false))
	})
	require.NoError(err)
	defer func() { require.NoError(consumer.Cancel(consumer2)) }()

	t.Log("Cancelling consumer1")
	// should be safe to call it multiple times
	err = consumer.Cancel(consumer1)
	require.NoError(err)
	err = consumer.Cancel(consumer1)
	require.NoError(err)

	t.Log("Drop and prevent connections to RabbitMQ to show consumer1 will not be registered again.")
	amqp.DisableConnections(t)
	t.Log("Allow connections to RabbitMQ to start the registration of consumers.")
	amqp.EnableConnections(t)

	t.Log("Sending second message.")
	// message after cancellation should NOT be received by consumer1
	amqp.Publish(t, queue, "msg2")

	t.Log("Waiting on messages...")
	select {
	case <-time.After(timeout):
		require.FailNow("Timed out waiting on messages.")
	case got := <-msg:
		require.Equal("msg2", got)
		t.Logf("Received message %q", got)
	}

	assert.EqualValuesf(int32(1), messageCountConsumer1, "First consumer got %d messages, want %d instead", messageCountConsumer1, 1)
}

func TestClose(t *testing.T) {
	require := require.New(t)

	amqp := inttest.SetupRabbitMQ(t)

	consumer, err := rabbitmq.NewConsumer(amqp.ProxiedURI,
		rabbitmq.WithConnectionName(t.Name()),
		rabbitmq.WithConsumerTagPrefix(t.Name()),
		rabbitmq.WithLogger(slog.New(slog.NewTextHandler(os.Stdout, nil))),
	)
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

	tm := time.NewTimer(timeout)
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

type temporary interface {
	Temporary() bool
}
