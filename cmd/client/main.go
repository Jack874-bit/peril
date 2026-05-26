package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func publishGameLog(gl routing.GameLog, ch *amqp.Channel) error {
	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+gl.Username, gl)
	if err != nil {
		return err
	}
	return nil
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {

	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(am)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+gs.Player.Username, gamelogic.RecognitionOfWar{
				Attacker: am.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackRequeue
		}
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(row gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(row)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			err := publishGameLog(routing.GameLog{
				CurrentTime: time.Now(),
				Message:     fmt.Sprintf("%v won a war against %v", winner, loser),
				Username:    row.Attacker.Username,
			}, ch)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			err := publishGameLog(routing.GameLog{
				CurrentTime: time.Now(),
				Message:     fmt.Sprintf("%v won a war against %v", winner, loser),
				Username:    row.Attacker.Username,
			}, ch)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			err := publishGameLog(routing.GameLog{
				CurrentTime: time.Now(),
				Message:     fmt.Sprintf("A war between %v and %v resulted in a draw", winner, loser),
				Username:    row.Attacker.Username,
			}, ch)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Println("error: unknown war outcome")
			return pubsub.NackDiscard
		}
	}
}

func main() {
	connectionString := "amqp://guest:guest@localhost:5672/"
	newConnection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Unable to create new connection")
	}
	defer newConnection.Close()

	newChan, err := newConnection.Channel()
	if err != nil {
		log.Fatalf("Unable to create new channel")
	}

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Unablet to welcome client")
	}
	state := gamelogic.NewGameState(userName)
	pubsub.SubscribeJSON(newConnection, routing.ExchangePerilDirect, routing.PauseKey+"."+userName, routing.PauseKey, pubsub.SimpleQueueTransient, handlerPause(state))
	pubsub.SubscribeJSON(newConnection, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+userName, routing.ArmyMovesPrefix+".*", pubsub.SimpleQueueTransient, handlerMove(state, newChan))
	pubsub.SubscribeJSON(newConnection, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix, routing.WarRecognitionsPrefix+".*", pubsub.SimpleQueueDurable, handlerWar(state, newChan))

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "spawn":
			state.CommandSpawn(words)
		case "move":
			newMove, err := state.CommandMove(words)
			if err != nil {
				log.Print("failed command move")
				continue
			}
			err = pubsub.PublishJSON(newChan, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+userName, newMove)
			if err != nil {
				log.Print("failed move")
				continue
			}
		case "status":
			state.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) < 2 {
				log.Println("Unknown quantity")
				continue
			}
			spamNum, err := strconv.Atoi(words[1])
			if err != nil {
				log.Println("Unable to convert number")
				continue
			}
			for i := 0; i < spamNum; i++ {
				logString := gamelogic.GetMaliciousLog()
				err := pubsub.PublishGob(newChan, routing.ExchangePerilTopic, routing.GameLogSlug+"."+userName, routing.GameLog{
					CurrentTime: time.Now(),
					Message:     logString,
					Username:    userName,
				})
				if err != nil {
					log.Println("Failed to spam")
				}
			}

		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}
