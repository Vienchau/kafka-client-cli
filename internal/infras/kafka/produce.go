package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func (s *store) ProduceMessage(ctx context.Context, key string, payload []byte, headers map[string]string) error {
	writer := s.writerDial()

	msg := kafka.Message{
		Key:   []byte(key),
		Value: payload,
	}

	if headers != nil {
		if len(headers) > 0 {
			headersKafka := make([]kafka.Header, 0)
			for k, v := range headers {
				headersKafka = append(
					headersKafka,
					kafka.Header{
						Key:   k,
						Value: []byte(v),
					},
				)
			}

			msg.Headers = headersKafka
		}
	}
	err := writer.WriteMessages(
		ctx,
		msg,
	)

	if err != nil {
		return err
	}

	fmt.Println("Message produced successfully!")
	return writer.Close()
}
