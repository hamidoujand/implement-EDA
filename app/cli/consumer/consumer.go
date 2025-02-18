package main

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/hamidoujand/eda/pkg/rabbitmq"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	//just like producer we in here need 2 connections when using "rpc"

	producer, err := rabbitmq.Connect(
		"hamid",
		"password",
		"localhost",
		"customers",
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer producer.Close()

	client, err := rabbitmq.Connect(
		"hamid",
		"password",
		"localhost",
		"customers",
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer client.Close()

	//now its up to consumer to create the queue and it must be unname
	queue, err := client.CreateQueue("", true, true)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	//now we bind this queue to the exchange we want to listen to
	//we do not want to pass "key" anymore, because we are in fanout
	err = client.CreateBinding(queue.Name, "", "customer_events")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	//consume the queue we just randomly created
	messageBus, err := client.Consume(
		queue.Name,
		"email_service", //this is the email service that used to send emails, you can name it anything but unique
		false,
	)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	//we want to tell the server we only can accept 10 messages at the time
	//and do not push more, pass 0 and bytes will be ignored because here we dealing
	//with number of messages
	if err := client.ApplyQos(10, 0, true); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	// defer cancel()

	workers := 10
	workerPool := make(chan amqp091.Delivery, workers)
	var wg sync.WaitGroup
	wg.Add(workers)

	//we need to range over the messages spawn a new goroutine
	go func() {
		defer close(workerPool)

		for msg := range messageBus {
			workerPool <- msg
		}

	}()

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for msg := range workerPool {
				fmt.Println("working on the message:", string(msg.Body))
				//here after we received the message we can publish to producer
				if err := producer.Send(
					context.Background(),
					"customer_callbacks",
					msg.ReplyTo, //we send to the "replyTo" field which producer defined
					amqp091.Publishing{
						ContentType:   "text/html",
						DeliveryMode:  amqp091.Persistent,
						Body:          []byte("all good."),
						CorrelationId: msg.CorrelationId, //so publisher knows what message this response if for
					},
				); err != nil {
					fmt.Println("failed to send callback: %w", err)
					return
				}
				if err := msg.Ack(false); err != nil {
					fmt.Println("failed to ack:", err)
					return
				}

			}
		}()
	}

	wg.Wait()
	fmt.Println("done son")
}

// this is the version 1
func consumeMessageV1(messageBus <-chan amqp091.Delivery) {
	// blocking channel
	var blockingCh chan struct{}

	// create a goroutine to consume the messages
	go func() {
		for msg := range messageBus {
			fmt.Printf("message: %s\n", string(msg.Body))

			//we can check to see if message is here for second time, and because
			//we could not ack it the first time
			if !msg.Redelivered {
				//if message comes here for the first time we are going to pass
				//and not going to "Ack" and then ask to "requeue" it and send
				//it again .

				//NOTE: this is usefull when our server fails and we can ask manually
				//to get the same message back
				msg.Nack(false, true)
				continue

			}

			if err := msg.Ack(false); err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("message %s: acked\n", msg.MessageId)
		}
	}()
	fmt.Println("consuming messages..., CTRL+C to exit.")
	<-blockingCh
}
