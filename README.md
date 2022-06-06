# Instance Queue

Queue library used by the DHIS2 instance manager for producing/consuming
messages from [RabbitMQ](https://www.rabbitmq.com/).

## Design

This queue library is a higher level library built on top of
[amqp091-go](https://github.com/rabbitmq/amqp091-go). It is designed for the
DHIS2 instance manager use cases. It might thus not be sufficient for your use
cases. However, it might help you in designing your own library :smile:

Some things we considered/implemented

* encapsulate best practices we found in our research when working with RabbitMQ
  * use separate connections for producer/consumer
  * be sparse with creating connections, they are designed to be long lived
  * be sparse with creating channels, they do not map 1:1 on-to threads and
    defintiely not onto goroutines
* re-connect/re-open channel on disconnect or channel exceptions
  see https://github.com/rabbitmq/amqp091-go/issues/40
* consumer tag prefix for easier debugging on consumers

## Getting started

Setup a local RabbitMQ

```sh
make dev
```

Setup a consumer using the instance queue library

```sh
go run cmd/consume/main.go
```

Setup a producer using the amqp library (will be replaced with queue library
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
  docker container proxying requests to RabbitMQ. This approach will drop the
  connection of consumer and producer.

```sh
docker exec -it rabbitmq-proxy-1 /toxiproxy-cli toggle rabbitmq
```

Refer to https://github.com/Shopify/toxiproxy for more nasty disruptions you
can play with :smirk:

Note: the consumer will re-connect but not re-consume (see
[Limitations](#limitations)).

## Test

Run all tests using

```sh
make test
```

Run an individual test using

```sh
go test -v -race -run TestSuiteConsumer . -testify.m TestReconnectConsumerConnection
```

## Limitations

* producer is not yet implement, but will be soon!
* logging cannot be configured/turned off. We could define an interface for the
  libraries logging needs. Clients can then pass in any logger. Can be as
  simple as https://github.com/go-redis/redis/pull/1285
* re-connection logic does not give you any insight into its state. We could
  provide callbacks that are invoked on disconnect/reconnect like
  https://pkg.go.dev/github.com/nats-io/nats.go#DisconnectErrHandler
  https://pkg.go.dev/github.com/nats-io/nats.go#ReconnectHandler
* only way to stop re-connection logic is to call Close(). We could provide a
  MaxReconnect option.
* receive() does not allow you to return any errors encountered while
  processing a message. We could change the signature to return an error and
  provide an optional error callback which will be invoked for every error.
* Consume() will not re-consume if the connection or channel drops, even
  though the connection/channel might be re-established due to the
  re-connection logic. You can implement a retry client side for the case that
  the consumer is disconnected at the time you try to Consume(). You will not
  be able to implement a retry client side for when the connection is dropped
  after you have called Consume(). Reason is not having any insight into the
  connection state mentioned above.
