package main

import (
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	if err := run(); err != nil {
		fmt.Printf("exit due to: %s", err)
		os.Exit(1)
	}
}

func run() error {
	queue := "queue_client_example"
	addr := "amqp://guest:guest@localhost:18081"

	c, err := amqp.Dial(addr)
	if err != nil {
		return err
	}
	ch, err := c.Channel()
	if err != nil {
		return err
	}
	fmt.Println("Connected, login to management console at http://localhost:15672/ using user/pw guest")

	// Attempt to push a message every 2 seconds
	for {
		time.Sleep(time.Second * 2)
		t := time.Now().String()
		correlationId := uuid.NewString()
		err = ch.Publish("", queue, false, false, amqp.Publishing{
			DeliveryMode:  amqp.Persistent,
			Body:          []byte(t),
			CorrelationId: correlationId,
		})
		if err != nil {
			fmt.Printf("Produce failed: %s\n", err)
		} else {
			fmt.Printf("produced: %q (%s)\n", t, correlationId)
		}
	}
}
