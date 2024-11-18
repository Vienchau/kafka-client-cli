package model

import (
	"encoding/json"
	"fmt"
)

const (
	COMPACT_JSON = 0
	BEAUTY_JSON  = 1
)

type Message struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Length    int32  `json:"length"`
}

func NewMessage(key, value string, partition int, offset int64) *Message {
	return &Message{
		Key:       key,
		Value:     value,
		Partition: partition,
		Offset:    offset,
		Length:    int32(len(value)),
	}
}

func (m *Message) toCompactJsonPrint() {
	jsonStr, _ := json.Marshal(m)
	fmt.Println(string(jsonStr))
}

func (m *Message) toBeautyJsonPrint() {
	jsonStr, _ := json.MarshalIndent(m, "", "  ")
	fmt.Println(string(jsonStr))
}

func (m *Message) toBulletListPrint() {
	fmt.Println("New message coming:")
	fmt.Printf("  - Key: %s\n", m.Key)
	fmt.Printf("  	- Value: %s\n", m.Value)
	fmt.Printf("  	- Partition: %d\n", m.Partition)
	fmt.Printf("  	- Offset: %d\n", m.Offset)
	fmt.Printf("  	- Length: %d\n", m.Length)
}

func (m *Message) Print(printOpt int) {
	switch printOpt {
	case COMPACT_JSON:
		m.toCompactJsonPrint()
	case BEAUTY_JSON:
		m.toBeautyJsonPrint()
	default:
		m.toBulletListPrint()
	}
}
