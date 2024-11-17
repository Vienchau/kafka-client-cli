package usecases

import "context"

type Consume struct {
	consumeRepo ConsumeRepository
}

type ConsumeRepository interface {
	ConsumeMessage(ctx context.Context, topic string) error
}

func NewConsumeUsercase(consumeRepo ConsumeRepository) *Consume {
	return &Consume{consumeRepo}
}

func (c *Consume) Execute(ctx context.Context, topic string) error {
	err := c.consumeRepo.ConsumeMessage(ctx, topic)
	if err != nil {
		return err
	}

	return nil
}
