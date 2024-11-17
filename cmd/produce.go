package cmd

import (
	"context"
	"fmt"
	"kcli/internal/infras/kafka"
	"kcli/internal/usecases"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var produceCmd = &cobra.Command{
	Use:     "produce",
	Aliases: []string{"p"},
	Short:   "Produce messages to a Kafka topic",
	Run:     produceCmdHandler,
}

func init() {
	produceCmd.PersistentFlags().StringP(
		"topic",
		"t",
		"",
		"[REQUIRED] Kafka topic to consume messages from",
	)

	produceCmd.PersistentFlags().StringP(
		"bootstrap-servers",
		"b",
		"",
		"[REQUIRED] Kafka bootstrap servers, split by ',' (e.g., 'localhost:9092,localhost:9093')")

	produceCmd.PersistentFlags().String(
		"username",
		"",
		"Username for authentication")

	produceCmd.PersistentFlags().String(
		"password",
		"",
		"Password for authentication")

	produceCmd.PersistentFlags().StringP(
		"key",
		"k",
		"",
		"Message key, if this option empty, the key will be generated automatically")

	produceCmd.PersistentFlags().StringP(
		"payload",
		"p",
		"",
		"Message payload")

	produceCmd.PersistentFlags().StringP(
		"file",
		"f",
		"",
		"Read message payload from file")

	produceCmd.MarkPersistentFlagRequired("bootstrap-servers")
	produceCmd.MarkPersistentFlagRequired("topic")
	produceCmd.MarkFlagsRequiredTogether("username", "password")
	produceCmd.MarkFlagsMutuallyExclusive("file", "payload")
}

func produceCmdHandler(cmd *cobra.Command, args []string) {
	// flag parsing
	topic, _ := cmd.Flags().GetString("topic")
	bootstrapServerStr, _ := cmd.Flags().GetString("bootstrap-servers")
	username, _ := cmd.Flags().GetString("username")
	password, _ := cmd.Flags().GetString("password")
	key, _ := cmd.Flags().GetString("key")
	payload, _ := cmd.Flags().GetString("payload")
	withFile, _ := cmd.Flags().GetString("with-file")

	// Validate input
	bootstrapServers := strings.Split(bootstrapServerStr, ",")

	var opts []kafka.StoreOption
	if username != "" && password != "" {
		opts = append(opts, kafka.WithAuthenticate(username, password))
	}

	store := kafka.NewKafkaStore(bootstrapServers, opts...)

	// Context for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	svc := usecases.NewProduceUsecase(store)
	err := svc.Execute(ctx, topic, key, []byte(payload), withFile)
	if err != nil {
		fmt.Println("Error while producing message: ", err)
		return
	}
}
