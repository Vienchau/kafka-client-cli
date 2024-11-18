package usecases

import "context"

type consume struct {
	consumeRepo ConsumeRepository
}

type ConsumeRepository interface {
	ConsumeMessage(ctx context.Context) error
}

func NewConsumeUsercase(consumeRepo ConsumeRepository) *consume {
	return &consume{consumeRepo}
}

func (c *consume) Execute(ctx context.Context) error {
	err := c.consumeRepo.ConsumeMessage(ctx)
	if err != nil {
		return err
	}

	return nil
}
