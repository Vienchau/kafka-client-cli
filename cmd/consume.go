package cmd

import (
	"context"
	"fmt"
	"kcli/internal/infras/kafka"
	"kcli/internal/usecases"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
)

type consumeOptions struct {
	BootstrapServers string
	Topic            string
	Username         string
	Password         string
}

var (
	consumeCmd = &cobra.Command{
		Use:     "consume",
		Aliases: []string{"c"},
		Short:   "Consume messages from a Kafka topic",
		Run:     consumeCmdHandler,
	}

	consumeOpts consumeOptions
)

func init() {
	consumeCmd.PersistentFlags().StringVar(
		&consumeOpts.BootstrapServers,
		"bootstrap-servers",
		"",
		"[REQUIRED] Kafka bootstrap servers, split by ',' (e.g., 'localhost:9092,localhost:9093')")

	consumeCmd.PersistentFlags().StringVar(
		&consumeOpts.Topic,
		"topic",
		"",
		"[REQUIRED] Kafka topic to consume messages from")

	consumeCmd.PersistentFlags().StringVar(
		&consumeOpts.Username,
		"username",
		"",
		"Username for authentication")

	consumeCmd.PersistentFlags().StringVar(
		&consumeOpts.Password,
		"password",
		"",
		"Password for authentication")

	consumeCmd.MarkPersistentFlagRequired("bootstrap-servers")
	consumeCmd.MarkPersistentFlagRequired("topic")
	consumeCmd.MarkFlagsRequiredTogether("username", "password")
}

func consumeCmdHandler(cmd *cobra.Command, args []string) {
	// Validate input
	bootstrapServers := strings.Split(consumeOpts.BootstrapServers, ",")

	var opts []kafka.StoreOption
	if consumeOpts.Username != "" && consumeOpts.Password != "" {
		opts = append(opts, kafka.WithAuthenticate(consumeOpts.Username, consumeOpts.Password))
	}

	store := kafka.NewKafkaStore(bootstrapServers, opts...)

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("Received shutdown signal, shutting down gracefully...")
		cancel()
	}()

	// Consume messages
	svc := usecases.NewConsumeUsercase(store)
	err := svc.Execute(ctx, consumeOpts.Topic)
	if err != nil {
		fmt.Println("Error while consuming messages: ", err)
		return
	}
}
