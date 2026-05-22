package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	connectionString := "amqp://guest:guest@localhost:5672/"
	newConnection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Unable to create new connection")
	}
	defer newConnection.Close()
	fmt.Println("Connection was successful")

	newChan, err := newConnection.Channel()
	if err != nil {
		log.Fatalf("Unable to create new channel")
	}
	err = pubsub.PublishJSON(newChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: true,
	})
	if err != nil {
		log.Fatalf("Unable to publish: %v", err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting down...")
}
