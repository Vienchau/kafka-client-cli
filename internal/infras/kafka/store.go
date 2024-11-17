package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type store struct {
	bootstrapServers []string
	isAuth           bool
	username         string
	password         string
}

// StoreOption is a function that configures a store.
type StoreOption func(*store)

// WithAuthenticate configures the store to authenticate with the given username and password.
func WithAuthenticate(username, password string) StoreOption {
	return func(s *store) {
		s.isAuth = true
		s.username = username
		s.password = password
	}
}

// NewKafkaStore creates a new kafka store.
func NewKafkaStore(bootstrapServers []string, opts ...StoreOption) *store {
	s := &store{
		bootstrapServers: bootstrapServers,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (s *store) writerDial() *kafka.Writer {

	kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      s.bootstrapServers,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 50,
		Async:        false,
	},
	)

	if !s.isAuth {
		return kafkaWriter
	}

	mechanism := plain.Mechanism{
		Username: s.username,
		Password: s.password,
	}
	sharedTransport := &kafka.Transport{
		SASL: mechanism,
	}
	kafkaWriter.Transport = sharedTransport

	return kafkaWriter
}

func (s *store) readerDial(topic string) *kafka.Reader {
	config := s.getReaderConfig(topic)
	if s.isAuth {
		mechanism := plain.Mechanism{
			Username: s.username,
			Password: s.password,
		}

		dialer := &kafka.Dialer{
			Timeout:       10 * time.Second,
			DualStack:     true,
			SASLMechanism: mechanism,
		}
		config.Dialer = dialer
	}

	return kafka.NewReader(config)
}

func (s *store) getReaderConfig(topic string) kafka.ReaderConfig {
	return kafka.ReaderConfig{
		Brokers:                s.bootstrapServers,
		Topic:                  topic,
		MinBytes:               5,
		MaxBytes:               10e6,
		MaxWait:                60 * time.Second,
		QueueCapacity:          1000,
		WatchPartitionChanges:  true,
		PartitionWatchInterval: 5 * time.Second,
		StartOffset:            kafka.LastOffset,
		CommitInterval:         time.Millisecond * 500,
	}
}
