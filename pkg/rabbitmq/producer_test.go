package rabbitmq

import (
	"fmt"
	"log/slog"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/rabbitmq"
	"github.com/stretchr/testify/require"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	// Given
	p := rabbitmq.Preset(
		rabbitmq.WithUser("guest", "guest"),
	)
	container, err := gnomock.Start(p)
	require.NoError(t, err)
	defer func() { require.NoError(t, gnomock.Stop(container)) }()
	uri := fmt.Sprintf("amqp://%s:%s@%s", "guest", "guest", container.DefaultAddress())

	// When
	producer := NewProducer(slog.Default(), uri)
	payload := struct{ ID uint }{uint(123)}
	err = producer.Produce("ttl-destroy", "correlationId", payload)
	require.NoError(t, err)

	// Then
	conn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer func() { require.NoError(t, ch.Close()) }()

	q, err := ch.QueueDeclare(
		"ttl-destroy",
		false, // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	require.NoError(t, err)

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	require.NoError(t, err)

	m := <-msgs
	require.Equal(t, []byte(`{"ID":123}`), m.Body)
	require.Equal(t, "correlationId", m.CorrelationId)
}
