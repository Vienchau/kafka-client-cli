package kafka

import (
	"context"
	"fmt"
	"log"
)

func (s *store) ConsumeMessage(ctx context.Context, topic string) error {
	reader := s.readerDial(topic)

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Csontext cancelled, stopping consumption...")
			err := reader.Close()
			if err != nil {
				return err
			}

			fmt.Println("Reader closed successfully!")
			return nil

		default:
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				return err
			}

			log.Println(string(m.Value))
		}
	}
}
