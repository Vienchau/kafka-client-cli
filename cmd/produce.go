package cmd

import (
	"fmt"

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
		"Kafka topic to consume messages from",
	)

	produceCmd.PersistentFlags().StringP(
		"bootstrap-servers",
		"b",
		"",
		"Kafka bootstrap servers, split by ',' (e.g., 'localhost:9092,localhost:9093')")

	produceCmd.PersistentFlags().StringP(
		"authen-options",
		"a",
		"",
		"Authentication options (e.g., 'username:password')")

	produceCmd.PersistentFlags().StringP(
		"format",
		"f",
		"json",
		"Message format (e.g., 'json', 'string')")

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
		"from-file",
		"ff",
		"",
		"Read message payload from file")
}

func produceCmdHandler(cmd *cobra.Command, args []string) {
	// flag parsing
	topic, _ := cmd.Flags().GetString("topic")
	bootstrapServerStr, _ := cmd.Flags().GetString("bootstrap-servers")
	authenOpts, _ := cmd.Flags().GetString("authen-options")

	bootstrapServers, err := validateBootstrapServers(bootstrapServerStr)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if topic == "" {
		fmt.Println(ErrTopicEmpty)
		return
	}

	fmt.Println("Produce message to topic: ", topic)
	fmt.Println("Bootstrap servers: ", bootstrapServers)
	fmt.Println("Authen options: ", authenOpts)
}
