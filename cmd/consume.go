package cmd

import (
	"context"
	"fmt"
	"kcli/internal/infras/kafka"
	"kcli/internal/usecases"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

type consumeOptions struct {
	BootstrapServers []string
	Topic            string
	Username         string
	Password         string
	GroupId          string
	Partition        int
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
	consumeCmd.PersistentFlags().StringSliceVarP(
		&consumeOpts.BootstrapServers,
		"bootstrap-servers",
		"b",
		[]string{},
		"[REQUIRED] Kafka bootstrap servers, split by ',' (e.g., 'localhost:9092,localhost:9093' - comma separated)")

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

	consumeCmd.PersistentFlags().StringVar(
		&consumeOpts.GroupId,
		"group-id",
		"kcli-group",
		"Group ID for consumer group, default is 'kcli-group'")

	consumeCmd.PersistentFlags().IntVar(
		&consumeOpts.Partition,
		"partition",
		-1,
		"Partition to consume messages from")

	consumeCmd.MarkPersistentFlagRequired("bootstrap-servers")
	consumeCmd.MarkPersistentFlagRequired("topic")
	consumeCmd.MarkFlagsRequiredTogether("username", "password")
	consumeCmd.MarkFlagsRequiredTogether("group-id", "partition")
}

func consumeCmdHandler(cmd *cobra.Command, args []string) {
	// Validate input
	bootstrapServers := consumeOpts.BootstrapServers

	for _, server := range bootstrapServers {
		fmt.Println("Server: ", server)
	}

	var opts []kafka.StoreOption
	if consumeOpts.Username != "" && consumeOpts.Password != "" {
		opts = append(opts, kafka.WithAuthenticate(consumeOpts.Username, consumeOpts.Password))
	}

	// if partition is not provided, use group ID
	if consumeOpts.Partition != -1 {
		opts = append(opts, kafka.WithPartition(consumeOpts.Partition))
	} else {
		opts = append(opts, kafka.WithGroupId(consumeOpts.GroupId))
	}

	store := kafka.NewKafkaStore(bootstrapServers, consumeOpts.Topic, opts...)

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
	err := svc.Execute(ctx)
	if err != nil {
		fmt.Println("Error while consuming messages: ", err)
		return
	}
}
