# TODO

# Consume

* make the queue durable. test messages are not lost if there is no consumer.

# Publish

* implement a client that can publish

# Tests

* setup logger for tests. So we can clearly differentiate it with the clients logs.
  Maybe use https://pkg.go.dev/github.com/testcontainers/testcontainers-go#TestLogger
* what is a good use of context in tests. testcontainers accept a context in their signatures.
Should we create a context with timeouts that make sense to us? or should these also take into
account what the `-timeout` flag is with which the tests are run?
