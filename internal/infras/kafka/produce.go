package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func (s *store) ProduceMessage(ctx context.Context, key string, payload []byte) error {
	writer := s.writerDial()

	err := writer.WriteMessages(
		ctx,
		kafka.Message{
			Key:   []byte(key),
			Value: payload,
		},
	)

	if err != nil {
		return err
	}

	fmt.Println("Message produced successfully!")
	return writer.Close()
}
