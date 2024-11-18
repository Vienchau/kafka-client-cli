package kafka

import (
	"context"
	"fmt"
)

func (s *store) ConsumeMessage(ctx context.Context) error {
	reader := s.readerDial()

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println(string(m.Value))
	}
	return reader.Close()
}
