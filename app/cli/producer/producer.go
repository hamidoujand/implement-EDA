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
	client, err := rabbitmq.Connect("hamid",
		"password",
		"localhost",
		"customers",
	)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer client.Close()
	//because we are in a 'pub sub' exchange, the publisher wont create the queue and bingings
	//the consumer must create them

	fmt.Println("connected")

	//send the message
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	if err := client.Send(
		ctx, "cutomer_events",
		"customer.created.us",
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent, //means if service went down the message will be send after service comes up
			Body:         []byte("some serious fucking goig on"),
		},
	); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

}
