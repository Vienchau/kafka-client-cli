package cmd

import (
	"context"
	"fmt"
	"kcli/internal/common"
	"kcli/internal/infras/kafka"
	"kcli/internal/usecases"
	"strings"
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
	Headers          []string
	File             string
	Timeout          int
}

func (opts *produceOptions) prettyPrint() {
	fmt.Println("Consumer will run with the following options:")
	fmt.Printf("Bootstrap Servers: %v\n", opts.BootstrapServers)
	fmt.Printf("Topic: %s\n", opts.Topic)
	if opts.Username != "" && opts.Password != "" {
		fmt.Printf("Username: %s\n", opts.Username)
		fmt.Printf("Password: %s\n", common.MaskPasswordStdOut(opts.Password))
	}
	if opts.Key != "" {
		fmt.Printf("Key: %s\n", opts.Key)
	}

	if opts.Payload != "" {
		fmt.Printf("Payload: %s\n", opts.Payload)
	}

	if opts.File != "" {
		fmt.Printf("Payload: %s\n", opts.File)
	}
	if len(opts.Headers) > 0 {
		fmt.Printf("Headers: %v\n", opts.Headers)
	}
	fmt.Printf("Timeout: %d\n", opts.Timeout)
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
		"Message key, if this option empty, the key will be generated automatically in uuid format")

	produceCmd.PersistentFlags().StringVarP(
		&produceOpts.Payload,
		"payload",
		"p",
		"",
		"Message payload, this command will read the payload content and compact it if content in json format")

	produceCmd.PersistentFlags().StringVarP(
		&produceOpts.File,
		"file",
		"f",
		"",
		"Read message payload from file, this command will read the file content and compact it if content in json format")

	produceCmd.PersistentFlags().IntVar(
		&produceOpts.Timeout,
		"with-timeout",
		5,
		"Timeout for producing message in second")

	produceCmd.PersistentFlags().StringSliceVar(
		&produceOpts.Headers,
		"header",
		[]string{},
		"Headers to include in the message, format: key:value")

	produceCmd.MarkPersistentFlagRequired("bootstrap-servers")
	produceCmd.MarkPersistentFlagRequired("topic")
	produceCmd.MarkFlagsRequiredTogether("username", "password")
	produceCmd.MarkFlagsMutuallyExclusive("file", "payload")
	produceCmd.MarkFlagsOneRequired("file", "payload")
}

func parseHeaders(headers []string) map[string]string {
	headerMap := make(map[string]string)
	for _, header := range headers {
		parts := strings.SplitN(header, ":", 2)
		if len(parts) == 2 {
			headerMap[parts[0]] = parts[1]
		}
	}
	return headerMap
}

func produceCmdHandler(cmd *cobra.Command, args []string) {
	// Validate input
	if len(produceOpts.BootstrapServers) == 0 {
		fmt.Println("Please provide bootstrap servers")
		return
	}

	if produceOpts.Topic == "" {
		fmt.Println("Please provide topic")
		return
	}

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

	// Pretty print options
	produceOpts.prettyPrint()
	headers := parseHeaders(produceOpts.Headers)

	// Produce message
	svc := usecases.NewProduceUsecase(store)
	err := svc.Execute(
		ctx,
		produceOpts.Key,
		[]byte(produceOpts.Payload),
		produceOpts.File,
		headers)

	if err != nil {
		fmt.Println("Error while producing message: ", err)
		return
	}
}
