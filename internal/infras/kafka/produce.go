package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func (s *store) ProduceMessage(ctx context.Context, topic, key string, payload []byte) error {
	fmt.Println("message json: ", string(payload))

	writer := s.writerDial()

	err := writer.WriteMessages(
		ctx,
		kafka.Message{
			Topic: topic,
			Key:   []byte(key),
			Value: payload,
		},
	)

	if err != nil {
		return err
	}

	return nil
}
