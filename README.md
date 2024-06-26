# RabbitMQ Client

RabbitMQ client library used by the DHIS2 instance manager for producing/consuming messages from
[RabbitMQ](https://www.rabbitmq.com/).

## Design

This RabbitMQ client library is a higher level library built on top of
[amqp091-go](https://github.com/rabbitmq/amqp091-go). It is designed for the
DHIS2 instance manager use cases. It might thus not be sufficient for your use
cases. However, it might help you in designing your own library :smile:

Some things we considered/implemented

* encapsulate best practices we found in our research when working with RabbitMQ
  * use separate connections for producer/consumer
  * be sparse with creating connections, they are designed to be long lived
  * be sparse with creating channels, they do not map 1:1 on-to threads and
    definitely not onto goroutines
* re-connect/re-open channel on disconnect or channel exceptions
  see https://github.com/rabbitmq/amqp091-go/issues/40
* re-register consumers after connection/channel have been re-established
* allow setting a connection name for easier debugging of consumers
* allow setting a consumer tag prefix for easier debugging of consumers

## Getting started

Setup a local RabbitMQ

```sh
make dev
```

Setup a consumer using the RabbitMQ client library

```sh
go run cmd/consumer/main.go
```

Setup a producer using the amqp library (might be replaced with RabbitMQ client library
implementation once its done)

```sh
go run cmd/producer/main.go
```

The consumer should now regularly receive messages from the producer.

You can test the re-connection logic of the consumer by closing the connection
either via

1. the management console at http://localhost:15672/ using user/pw guest. This
   allows you to drop the connection of consumer/producer individually.
2. or [toxiproxy](https://github.com/Shopify/toxiproxy) which is running in a
  docker container proxying requests to RabbitMQ.

To drop/allow connections from the [consumer](./cmd/consumer/main.go) do

```sh
docker compose exec proxy /toxiproxy-cli toggle rabbitmq_consumer
```

To drop/allow connections from the [producer](./cmd/producer/main.go) do

```sh
docker compose exec proxy /toxiproxy-cli toggle rabbitmq_producer
```

Note: the example producer will not re-connect because of its [limitations](#limitations).

Refer to https://github.com/Shopify/toxiproxy for more nasty disruptions you can play with :smirk:

## Test

Run all tests using

```sh
make test
```

Run an individual tests using `go test -run`.

## Limitations

* producer is not implement in the same way as consumer. This means it does not re-connect for you.
We don't know if we will implement the producer in the same way as the consumer.
* re-connection logic does not give you any insight into its state. We could
  provide callbacks that are invoked on disconnect/reconnect like
  https://pkg.go.dev/github.com/nats-io/nats.go#DisconnectErrHandler
  https://pkg.go.dev/github.com/nats-io/nats.go#ReconnectHandler
* only way to stop re-connection logic is to call Close(). We could provide a
  MaxReconnect option.
* receive() does not allow you to return any errors encountered while
  processing a message. We could change the signature to return an error and
  provide an optional error callback which will be invoked for every error.
* re-registering consumers might block as we don't shield against it ourselves or use
context.Context. We would need to check the amqp libs implementation of every method we use as their
new methods using Context do not all seem to be correctly using it https://github.com/rabbitmq/amqp091-go/issues/195
* we do not completey abstract away from [amqp091-go](https://github.com/rabbitmq/amqp091-go) as you
  can see in the signature of the receiver passed to Consume(). This is something we could do as we
  do not expose any other AMQP library specifics.
* we do not support different topologies. We only consume from the default exchange.
* we are logging using [slog](https://pkg.go.dev/log/slog). You can pass in an `slog.Logger`
otherwise we are using the `slog.Default()`.
