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
		"Kafka topic to consume messages from",
	)

	consumeCmd.PersistentFlags().StringP(
		"bootstrap-servers",
		"b",
		"",
		"Kafka bootstrap servers, split by ',' (e.g., 'localhost:9092,localhost:9093')")

	consumeCmd.PersistentFlags().StringP(
		"authen-options",
		"a",
		"",
		"Authentication options (e.g., 'username:password')")
}

func consumeCmdHandler(cmd *cobra.Command, args []string) {
	// flag parsing
	topic, _ := cmd.Flags().GetString("topic")
	bootstrapServerStr, _ := cmd.Flags().GetString("bootstrap-servers")
	authenOpts, _ := cmd.Flags().GetString("authen-options")

	// Validate input
	bootstrapServers, err := validateBootstrapServers(bootstrapServerStr)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if topic == "" {
		fmt.Println(ErrTopicEmpty.Error())
		return
	}

	var opts []kafka.StoreOption
	if len(authenOpts) != 0 {
		strs := strings.Split(authenOpts, ":")
		if len(strs) != 2 {
			fmt.Println(ErrInvalidAuthenOpts)
			return
		}

		username := strs[0]
		password := strs[1]
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

	// Consume message withj new usecase
	svc := usecases.NewConsumeUsercase(store)

	err = svc.Execute(ctx, topic)
	if err != nil {
		fmt.Printf(ErrTopicEmpty.Error(), err)
		return
	}

	fmt.Println("Consume message done!")
}
