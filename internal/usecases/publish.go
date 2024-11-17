package usecases

import "context"

type Produce struct {
	produceRepo ProduceRepository
}

type ProduceRepository interface {
	ProduceMessage(ctx context.Context, topic, key string, payload []byte) error
}

func NewPublishUsercase(produceRepo ProduceRepository) *Produce {
	return &Produce{produceRepo}
}

func (p *Produce) Execute(ctx context.Context, topic string, key string, payload []byte) error {
	err := p.produceRepo.ProduceMessage(ctx, topic, key, payload)
	if err != nil {
		return err
	}

	return nil
}
