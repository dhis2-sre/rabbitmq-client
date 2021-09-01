package queue

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

type Consumer interface {
	Launch()
	channel() string
	consume(d amqp.Delivery)
}

type AbstractConsumer struct {
	Consumer
	url string
}

func (a *AbstractConsumer) Launch() {
	log.Printf("Launching Consumer for: %s", a.channel())

	conn, err := amqp.Dial(a.url)
	if err != nil {
		log.Println(err)
		return
	}
	defer func(conn *amqp.Connection) {
		err := conn.Close()
		if err != nil {
			log.Println(err)
			return
		}
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		log.Println(err)
		return
	}
	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {
			log.Println(err)
			return
		}
	}(ch)

	q, err := ch.QueueDeclare(
		a.channel(),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Println(err)
		return
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Println(err)
		return
	}

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			a.consume(d)
		}
	}()
	<-forever
}
