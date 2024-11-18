package kafka

import (
	"context"
	"fmt"
	"kcli/internal/usecases/model"
)

func (s *store) ConsumeMessage(ctx context.Context, msgChan chan<- model.Message) error {
	reader := s.readerDial()

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println(err)
			break
		}

		msg := model.NewMessage(
			string(m.Key),
			string(m.Value),
			m.Partition,
			m.Offset,
		)
		msgChan <- *msg

	}
	err := reader.Close()
	if err != nil {
		return err
	}

	fmt.Println("consumer successfully!")
	return nil
}
