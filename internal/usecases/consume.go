package usecases

import (
	"context"
	"fmt"
	"kcli/internal/usecases/model"
)

type consume struct {
	consumeRepo ConsumeRepository
}

type ConsumeRepository interface {
	ConsumeMessage(ctx context.Context, msgChan chan<- model.Message) error
}

func NewConsumeUsercase(consumeRepo ConsumeRepository) *consume {
	return &consume{consumeRepo}
}

func (c *consume) Execute(ctx context.Context, printOpt int) error {
	msgChan := make(chan model.Message)
	defer close(msgChan)

	go func() {
		err := c.consumeRepo.ConsumeMessage(ctx, msgChan)
		if err != nil {
			fmt.Println("Error consuming messages:", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-msgChan:
			// Process the message
			msg.Print(printOpt)
		}
	}
}
