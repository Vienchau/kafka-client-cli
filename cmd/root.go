package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var (
	buildTime string
	version   string
)

var (
	ErrBootstrapServersEmpty = fmt.Errorf("bootstrap servers must not be empty")
	ErrTopicEmpty            = fmt.Errorf("topic must not be empty")
	ErrInvalidAuthenOpts     = fmt.Errorf("invalid authen options")
)

var rootCmd = &cobra.Command{
	Use:   "kcli",
	Short: "kcli is a simple Kafka client for publishing and subscribing to messages",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Oops. An error while executing kcli '%s'\n", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(produceCmd)
	rootCmd.AddCommand(consumeCmd)
	rootCmd.AddCommand(versionCmd)
}

func validateBootstrapServers(servers string) ([]string, error) {
	servers = strings.TrimSpace(servers)
	if len(servers) == 0 {
		return nil, ErrBootstrapServersEmpty
	}

	return strings.Split(servers, ","), nil
}
