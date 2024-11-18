package cmd

import (
	"context"
	"fmt"
	"kcli/internal/infras/kafka"
	"kcli/internal/usecases"
	"time"

	"github.com/spf13/cobra"
)

type produceOptions struct {
	BootstrapServers []string
	Topic            string
	Username         string
	Password         string
	Key              string
	Payload          string
	File             string
	Timeout          int
}

var (
	produceCmd = &cobra.Command{
		Use:     "produce",
		Aliases: []string{"p"},
		Short:   "Produce messages to a Kafka topic",
		Run:     produceCmdHandler,
	}

	produceOpts produceOptions
)

func init() {
	produceCmd.PersistentFlags().StringSliceVarP(
		&produceOpts.BootstrapServers,
		"bootstrap-servers",
		"b",
		[]string{},
		"[REQUIRED] Kafka bootstrap servers, split by ',' (e.g., 'localhost:9092,localhost:9093' - comma separated)")

	produceCmd.PersistentFlags().StringVar(
		&produceOpts.Topic,
		"topic",
		"",
		"[REQUIRED] Kafka topic to produce messages to")

	produceCmd.PersistentFlags().StringVar(
		&produceOpts.Username,
		"username",
		"",
		"Username for authentication")

	produceCmd.PersistentFlags().StringVar(
		&produceOpts.Password,
		"password",
		"",
		"Password for authentication")

	produceCmd.PersistentFlags().StringVarP(
		&produceOpts.Key,
		"key",
		"k",
		"",
		"Message key, if this option empty, the key will be generated automatically")

	produceCmd.PersistentFlags().StringVarP(
		&produceOpts.Payload,
		"payload",
		"p",
		"",
		"Message payload")

	produceCmd.PersistentFlags().StringVarP(
		&produceOpts.File,
		"file",
		"f",
		"",
		"Read message payload from file")

	produceCmd.PersistentFlags().IntVar(
		&produceOpts.Timeout,
		"with-timeout",
		5,
		"Timeout for producing message in second, default 5s")

	produceCmd.MarkPersistentFlagRequired("bootstrap-servers")
	produceCmd.MarkPersistentFlagRequired("topic")
	produceCmd.MarkFlagsRequiredTogether("username", "password")
	produceCmd.MarkFlagsMutuallyExclusive("file", "payload")
	produceCmd.MarkFlagsOneRequired("file", "payload")
}

func produceCmdHandler(cmd *cobra.Command, args []string) {
	// Validate input
	bootstrapServers := produceOpts.BootstrapServers

	var opts []kafka.StoreOption
	if produceOpts.Username != "" && produceOpts.Password != "" {
		opts = append(opts, kafka.WithAuthenticate(produceOpts.Username, produceOpts.Password))
	}

	store := kafka.NewKafkaStore(
		bootstrapServers,
		produceOpts.Topic,
		opts...)

	// Context for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(produceOpts.Timeout)*time.Second)
	defer cancel()

	// Produce message
	svc := usecases.NewProduceUsecase(store)
	err := svc.Execute(
		ctx,
		produceOpts.Key,
		[]byte(produceOpts.Payload),
		produceOpts.File)

	if err != nil {
		fmt.Println("Error while producing message: ", err)
		return
	}
}
