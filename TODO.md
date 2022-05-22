# TODO


# Consume

* test what happens if we cancel after re-connect. The cancel would happen with
  a consumer tag that does not exist on the new channel. Would that cause an
  exception and close the new channel?
* create logging interface and pass that to NewConsumer() and use logger instead of fmt.Print
* make the queue durable. test messages are not lost if there is no consumer.
* add dead-letter queue with policy on existing queue

# Publish

* implement a client that can publish

# Tests

* setup logger for tests. So we can clearly differentiate it with the clients logs.
  Maybe use https://pkg.go.dev/github.com/testcontainers/testcontainers-go#TestLogger
* think about how to best separate the test helpers. Having rabbitmq and toxiproxy
  helpers in the same package is a bit awkward with regards to their options.
  Own packages would be nice, but they should obviously not be included in the
  binary.
* skipping a test seems to start rabbitmq in the skipped test
* what is the default test timeout in go? seems like 10min. make sure tests fail earlier!
* test different scenarios and if tests would fail according to timeouts and
context cancelations

## Challenge

* I cannot populate toxiproxy using the same upstream twice. I would like to
  disturb producer and consumer separately like so

```json
 [
   {
    "name": "instance_queue_rabbitmq_consumer",
    "listen": "[::]:13306",
    "upstream": "rabbitmq:5672",
    "enabled": true
  },
  {
    "name": "instance_queue_rabbitmq_producer",
     "listen": "[::]:18080",
     "upstream": "rabbitmq:5672",
     "enabled": true
  }
]
```

This would be great for local testing in the docker compose setup and automated
integration tests.

