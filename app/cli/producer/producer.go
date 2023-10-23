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

	if err := client.CreateQueue("customer_created", true, false); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	//now we need to bind the queue
	if err := client.CreateBinding("customer_created",
		"customer.created.*",
		"customer_events"); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

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
