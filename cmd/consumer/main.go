package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jadson-medeiros/go-intensive/internal/order/infra/database"
	"github.com/jadson-medeiros/go-intensive/internal/order/usecase"
	"github.com/jadson-medeiros/go-intensive/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	db, err := sql.Open("sqlite3", "./orders.db")

	if err != nil {
		panic(err)
	}
	defer db.Close()
	repository := database.NewOrderRepository(db)
	uc := usecase.CalculatePriceUseCase{OrderRepository: repository}

	ch, err := rabbitmq.OpenChannel()

	if err != nil {
		panic(err)
	}

	defer ch.Close()

	out := make(chan amqp.Delivery) // Channel

	forever := make(chan bool)

	go rabbitmq.Consume(ch, out) // T2

	qtdWorkers := 5

	for i := 1; i <= qtdWorkers; i++ {
		go worker(out, &uc, i)
	}

	<-forever
}

func worker(deliveryMessage <-chan amqp.Delivery, uc *usecase.CalculatePriceUseCase, workerdID int) {
	for msg := range deliveryMessage {
		var inputDTO usecase.OrderInputDTO
		err := json.Unmarshal(msg.Body, &inputDTO)

		if err != nil {
			panic(err)
		}

		outputDTO, err := uc.Execute(inputDTO)

		if err != nil {
			panic(err)
		}

		msg.Ack(false)
		fmt.Printf("worker %d has processed order %s\n", workerdID, outputDTO.ID)
		time.Sleep(1 * time.Second)
	}
}
