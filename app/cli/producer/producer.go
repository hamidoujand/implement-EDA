package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/hamidoujand/eda/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	//when we deal with rpc "direct" exchange, we need 2 connection and client
	//one for producing and one for consumming from the client
	//this is used to send messages to the client
	producer, err := rabbitmq.Connect("hamid",
		"password",
		"localhost",
		"customers",
	)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer producer.Close()
	//we are going to use this to listen to the client messages
	consumer, err := rabbitmq.Connect("hamid",
		"password",
		"localhost",
		"customers",
	)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer consumer.Close()

	fmt.Println("connected")

	//now we need a queue to get messages from there
	queue, err := consumer.CreateQueue(
		"",
		true,
		true,
	)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	//bind this to the consume client
	if err := consumer.CreateBinding(
		queue.Name,
		queue.Name, //because this is a "direct" exchange, we need to pass "key" as the same as "queue name"
		"customer_callbacks"); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	messageBus, err := consumer.Consume(
		queue.Name,
		"consumer-api",
		true,
	)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	go func() {
		for msg := range messageBus {
			fmt.Println("message callback:", msg.CorrelationId)
		}
	}()

	//send the message
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	if err := producer.Send(
		ctx, "cutomer_events",
		"customer.created.us",
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent, //means if service went down the message will be send after service comes up
			Body:         []byte("some serious fucking goig on"),
			ReplyTo:      queue.Name, //here we saying to the consumer to where it can
			//send its messages and where publisher is waiting for callbacks
			CorrelationId: "customer_created_1", //this is for consumer to know which message to response with this id
		},
	); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("use CTRL + C to cancel.")

	var blocking chan struct{}
	<-blocking
}
