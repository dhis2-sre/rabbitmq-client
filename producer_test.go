package rabbitmq_test

import (
	"fmt"
	"time"

	rabbitmq "github.com/dhis2-sre/instance-queue"
	"github.com/google/uuid"
)

func (s *consumerSuite) TestProduce() {
	require := s.Require()
	// assert := s.Assert()

	producer, err := rabbitmq.NewProducer(s.rabbitURI)
	require.NoError(err)
	defer func() { require.NoError(producer.Close()) }()

	queue := "producer_test_queue_1"
	require.NoError(producer.Produce(queue, []byte("msg1")))

	_, err = s.amqpClient.ch.QueueDeclare(
		queue,
		false, // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	require.NoError(err)
	id := "producer_test-" + uuid.NewString()
	ds, err := s.amqpClient.ch.Consume(
		queue,
		id,    // Consumer
		false, // Auto-Ack
		false, // Exclusive
		false, // No-local
		false, // No-Wait
		nil,   // Args
	)
	require.NoError(err)

	msg := make(chan string)
	go func() {
		for d := range ds {
			msg <- string(d.Body)
			_ = d.Ack(false)
		}
	}()

	fmt.Println("Waiting on message consumption...")
	select {
	case <-time.After(s.timeout):
		require.FailNow("Timed out waiting on message consumption")
	case got := <-msg:
		require.Equal("msg1", got)
	}
}
