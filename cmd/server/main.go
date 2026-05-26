package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerLogs() func(routing.GameLog) pubsub.AckType {
	return func(gl routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gl)
		if err != nil {
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}

func main() {
	connectionString := "amqp://guest:guest@localhost:5672/"
	newConnection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Unable to create new connection")
	}
	defer newConnection.Close()
	fmt.Println("Connection was successful")

	gamelogic.PrintServerHelp()

	newChan, err := newConnection.Channel()
	if err != nil {
		log.Fatalf("Unable to create new channel")
	}

	err = pubsub.SubscribeGob(newConnection, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.SimpleQueueDurable, handlerLogs())
	if err != nil {
		log.Fatalf("could not declare and bind queue: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "pause":
			fmt.Println("Sending pause message")
			err = pubsub.PublishJSON(newChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
			if err != nil {
				log.Fatalf("Unable to publish: %v", err)
			}
		case "resume":
			fmt.Println("Sending resume message")
			err = pubsub.PublishJSON(newChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
			if err != nil {
				log.Fatalf("Unable to publish: %v", err)
			}
		case "quit":
			fmt.Println("Sending quit message")
			return
		default:
			fmt.Println("Unable to understand command")
		}
	}
}
