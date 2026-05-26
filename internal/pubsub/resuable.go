package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	valByte, err := json.Marshal(val)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        valByte,
	})
	if err != nil {
		return err
	}
	return nil
}

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	newCon, err := conn.Channel()
	if err != nil {
		log.Fatalf("Unable to create new channel")
	}
	durable := queueType == SimpleQueueDurable
	autoDelete := !durable
	exclusive := !durable

	newQueue, err := newCon.QueueDeclare(queueName, durable, autoDelete, exclusive, false, amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	})
	if err != nil {
		log.Fatalf("Unable to create new queue")
	}

	err = newCon.QueueBind(newQueue.Name, key, exchange, false, nil)
	if err != nil {
		log.Fatalf("unable to bind channel to queue")
	}

	return newCon, newQueue, nil

}

func subscriber[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	newChan, newQueue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	newChan.Qos(10, 0, false)
	delChan, err := newChan.Consume(newQueue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		defer newChan.Close()
		for mes := range delChan {

			msg, err := unmarshaller(mes.Body)
			if err != nil {
				log.Printf("Unable to decode message")
				continue
			}
			ackType := handler(msg)
			switch ackType {
			case Ack:
				mes.Ack(false)
				log.Println("Ack")
			case NackRequeue:
				mes.Nack(false, true)
				log.Println("Nack Requeue")
			case NackDiscard:
				mes.Nack(false, false)
				log.Println("Nack Discard")
			}
		}
	}()
	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	return subscriber(conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		var msg T
		err := json.Unmarshal(data, &msg)
		if err != nil {
			log.Printf("Unable to decode message")
			return msg, err
		}
		return msg, nil
	})
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var b bytes.Buffer
	encoder := gob.NewEncoder(&b)
	err := encoder.Encode(val)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        b.Bytes(),
	})
	if err != nil {
		return err
	}
	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	return subscriber(conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		buffer := bytes.NewBuffer(data)
		decoder := gob.NewDecoder(buffer)
		var msg T
		err := decoder.Decode(&msg)
		if err != nil {
			return msg, err
		}
		return msg, nil
	})
}
