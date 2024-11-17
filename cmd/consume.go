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

var consumeCmd = &cobra.Command{
	Use:     "consume",
	Aliases: []string{"c"},
	Short:   "Consume messages to a Kafka topic",
	Run:     consumeCmdHandler,
}

func init() {
	consumeCmd.PersistentFlags().StringP(
		"topic",
		"t",
		"",
		"[REQUIRED] Kafka topic to consume messages from",
	)

	consumeCmd.PersistentFlags().StringP(
		"bootstrap-servers",
		"b",
		"",
		"[REQUIRED] Kafka bootstrap servers, split by ',' (e.g., 'localhost:9092,localhost:9093')")

	consumeCmd.PersistentFlags().String(
		"username",
		"",
		"Username for authentication")

	consumeCmd.PersistentFlags().String(
		"password",
		"",
		"Password for authentication")

	consumeCmd.MarkPersistentFlagRequired("bootstrap-servers")
	consumeCmd.MarkPersistentFlagRequired("topic")
	consumeCmd.MarkFlagsRequiredTogether("username", "password")
}

func consumeCmdHandler(cmd *cobra.Command, args []string) {
	// flag parsing
	topic, _ := cmd.Flags().GetString("topic")
	bootstrapServerStr, _ := cmd.Flags().GetString("bootstrap-servers")
	username, _ := cmd.Flags().GetString("username")
	password, _ := cmd.Flags().GetString("password")

	// Validate input
	bootstrapServers := strings.Split(bootstrapServerStr, ",")

	var opts []kafka.StoreOption
	if username != "" && password != "" {
		opts = append(opts, kafka.WithAuthenticate(username, password))
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

	// Consume message with new usecase
	svc := usecases.NewConsumeUsercase(store)

	err := svc.Execute(ctx, topic)
	if err != nil {
		fmt.Printf(ErrTopicEmpty.Error(), err)
		return
	}

	fmt.Println("Consume message done!")
}
