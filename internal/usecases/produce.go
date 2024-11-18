package usecases

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"kcli/internal/common"
	"os"
)

type produce struct {
	produceRepo ProduceRepository
}

type ProduceRepository interface {
	ProduceMessage(ctx context.Context, key string, payload []byte) error
}

func NewProduceUsecase(produceRepo ProduceRepository) *produce {
	return &produce{produceRepo}
}

func (p *produce) Execute(ctx context.Context, key string, payload []byte, withFile string) error {
	if withFile != "" {
		data, err := os.ReadFile(withFile)
		if err != nil {
			return err
		}
		payload = data
	}

	// compact if payload is json
	if json.Valid(payload) {
		var compactPayload bytes.Buffer
		if err := json.Compact(&compactPayload, payload); err != nil {
			return err
		}
		payload = compactPayload.Bytes()
	}

	// generate key if empty
	if len(key) == 0 {
		key = common.GenerateUUID()
	}

	fmt.Println("Producing message with key:", key)

	err := p.produceRepo.ProduceMessage(ctx, key, payload)
	if err != nil {
		return err
	}

	return nil
}
