package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func Connect(username, password, host, vhost string) (Client, error) {
	uri := fmt.Sprintf("amqp://%s:%s@%s/%s", username, password, host, vhost)
	conn, err := amqp.Dial(uri)
	if err != nil {
		return Client{}, fmt.Errorf("dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return Client{}, fmt.Errorf("channel: %w", err)
	}
	//because we want to use "confirmation" on message delivery in producer
	//then we need to put the channel into confirm mode this way producer can
	//also ack the send operation

	//confirm mode
	if err := ch.Confirm(false); err != nil {
		return Client{}, fmt.Errorf("confirm: %w", err)
	}

	//create all the exchanges we need in here.
	err = ch.ExchangeDeclare("customer_callbacks",
		"direct", //its going to use RPC now
		true,
		false,
		false,
		false,
		amqp.Table{vhost: "customers", username: "hamid", password: "password"})

	if err != nil {
		return Client{}, fmt.Errorf("exchangeDeclare: %w", err)
	}

	return Client{conn: conn, ch: ch}, nil
}

// only close the channel, because others maybe using the underlying conn
func (c Client) Close() error {
	if err := c.ch.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return nil
}

func (c Client) CreateQueue(name string, durable, autodelete bool) (amqp.Queue, error) {
	//because we are going to use "fanout" exchange, we need to know the name
	//of the queue
	q, err := c.ch.QueueDeclare(
		name,
		durable,
		autodelete,
		false,
		false,
		nil)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("queueDeclare: %w", err)
	}

	return q, nil
}

func (c Client) CreateBinding(queueName, key, exchange string) error {
	return c.ch.QueueBind(
		queueName,
		key,
		exchange,
		false,
		nil,
	)
}

func (c Client) Send(ctx context.Context, exchange, routingKey string, opts amqp.Publishing) error {
	//Use this when your producer does not want to wait for all messages to be send

	// return c.ch.PublishWithContext(
	// 	ctx, exchange,
	// 	routingKey,
	// 	true,
	// 	false,
	// 	opts,
	// )

	//use this when your producer needs confirmation about the messages that went to consumer
	//all of them
	//NOTE: if you are using this , then you need configure your "queue" as well
	//to be in "confirm mode"
	confirmation, err := c.ch.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		routingKey,
		true,  //mandatory means if error happend return that error
		false, //deprecated option, just pass false
		opts,
	)

	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	//now we wait to get confirmation about the message that went and delivered to consumer
	//then we send next message
	confirmation.Wait()
	return nil
}

func (c Client) Consume(queueName, consumerId string, autoAck bool) (<-chan amqp.Delivery, error) {
	return c.ch.Consume(
		queueName,
		consumerId,
		autoAck,
		//if you set it to true then that means this service is the ONLY service that going messages and
		//do not send the messages to other services using a loadbalancer maybe you create multiple replica
		//of your consumer
		false,
		false, //its not supported in rabbitmq, its only used in amq
		false, //does not wait for server to confirm
		nil,
	)
}

//ApplyQos allow us to limit the number of "UnAcked" messages that can be enter
//to the queue and protect us from DOS attacks

func (c Client) ApplyQos(count, size int, global bool) error {
	return c.ch.Qos(count, size, global)
}
