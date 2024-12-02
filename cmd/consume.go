package cmd

import (
	"context"
	"fmt"
	"kcli/internal/common"
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
	PrintOpt         int
}

func (opts *consumeOptions) prettyPrint() {
	fmt.Println("Consumer will run with the following options:")
	fmt.Printf("Bootstrap Servers: %v\n", opts.BootstrapServers)
	fmt.Printf("Topic: %s\n", opts.Topic)
	if opts.Username != "" && opts.Password != "" {
		fmt.Printf("Username: %s\n", opts.Username)
		fmt.Printf("Password: %s\n", common.MaskPasswordStdOut(opts.Password))
	}
	if opts.Partition != -1 {
		fmt.Printf("Partition: %d\n", opts.Partition)
	} else {
		fmt.Printf("Group ID: %s\n", opts.GroupId)
	}

	fmt.Printf("Print Option: %d\n", opts.PrintOpt)
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

	consumeCmd.PersistentFlags().IntVar(
		&consumeOpts.PrintOpt,
		"print-opt",
		2,
		"Print option for consumed messages, 0: compact JSON, 1: beauty JSON, default: bullet list")

	consumeCmd.MarkPersistentFlagRequired("bootstrap-servers")
	consumeCmd.MarkPersistentFlagRequired("topic")
	consumeCmd.MarkFlagsRequiredTogether("username", "password")
	consumeCmd.MarkFlagsRequiredTogether("group-id", "partition")
}

func consumeCmdHandler(cmd *cobra.Command, args []string) {
	// Validate input
	if len(consumeOpts.BootstrapServers) == 0 {
		fmt.Println("Please provide bootstrap servers")
		return
	}

	if consumeOpts.Topic == "" {
		fmt.Println("Please provide topic")
		return
	}

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

	// Print options
	consumeOpts.prettyPrint()

	// Consume messages
	svc := usecases.NewConsumeUsercase(store)
	err := svc.Execute(ctx, consumeOpts.PrintOpt)
	if err != nil {
		fmt.Println("Error while consuming messages: ", err)
		return
	}

	fmt.Println("consumer closed successfully")
}
