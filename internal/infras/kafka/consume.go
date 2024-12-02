package kafka

import (
	"context"
	"fmt"
	"kcli/internal/usecases/model"

	"github.com/segmentio/kafka-go"
)

func extractMessageKafka(msg *kafka.Message) ([]byte, []byte, map[string]string) {
	key := msg.Key
	payload := msg.Value
	headersSlice := msg.Headers
	headerDicts := map[string]string{}
	for _, header := range headersSlice {
		headerDicts[header.Key] = string(header.Value)
	}

	return key, payload, headerDicts
}

func (s *store) ConsumeMessage(ctx context.Context, msgChan chan<- model.Message) error {
	reader := s.readerDial()

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println(err)
			break
		}

		key, payload, headerDicts := extractMessageKafka(&m)

		msg := model.NewMessage(
			string(key),
			string(payload),
			headerDicts,
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
