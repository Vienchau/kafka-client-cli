# Kafka CLI

> Kafka CLI (kcli) is a simple command-line tool for publishing and consuming messages with Kafka.

## Project structure

```
.
├── .gitignore              # Git ignore file
├── bin/                    # Binary output directory
│   ├── kcli                # Compiled binary
│   └── linux_amd64/        # Compiled binary for Linux AMD64 architecture
│       └── kcli
├── cmd/                    # Command-line interface commands
│   ├── consume.go          # Command for consuming messages
│   ├── produce.go          # Command for producing messages
│   ├── root.go             # Root command
│   └── version.go          # Version command
├── go.mod                  # Go module file
├── go.sum                  # Go dependencies file
├── internal/               # Internal packages
│   ├── common/             # Common utilities
│   │   └── utils.go
│   ├── infras/             # Infrastructure layer
│   │   └── kafka/          # Kafka related implementations
│   │       ├── consume.go
│   │       ├── produce.go
│   │       └── store.go
│   └── usecases/           # Use cases layer
│       ├── consume.go
│       ├── model/          # Models used in use cases
│       │   └── model.go
│       └── produce.go
├── main.go                 # Main entry point of the application
├── Makefile                # Makefile for building the project
```

## Build

```bash
make build
```

## Usage

```bash
$ ./bin/kcli

kcli is a simple Kafka client for publishing and subscribing to messages

Usage:
  kcli [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  consume     Consume messages from a Kafka topic
  help        Help about any command
  produce     Produce messages to a Kafka topic
  version     Print the version number of kcli

Flags:
  -h, --help   help for kcli

Use "kcli [command] --help" for more information about a command.
```

## Consume message

```bash
$ ./bin/kcli consume --help
Consume messages from a Kafka topic

Usage:
  kcli consume [flags]

Aliases:
  consume, c

Flags:
  -b, --bootstrap-servers strings   [REQUIRED] Kafka bootstrap servers, split by ',' (e.g., 'localhost:9092,localhost:9093' - comma separated)
      --group-id string             Group ID for consumer group, default is 'kcli-group' (default "kcli-group")
  -h, --help                        help for consume
      --partition int               Partition to consume messages from (default -1)
      --password string             Password for authentication
      --print-opt int               Print option for consumed messages, 0: compact JSON, 1: beauty JSON, default: bullet list (default 2)
      --topic string                [REQUIRED] Kafka topic to consume messages from
      --username string             Username for authentication

$ ./bin/kcli consume --bootstrap-servers='localhost:8097,localhost:8098,localhost:8099' --topic='be-application-test-01' --print-opt=1
```

## Produce message

```bash
$ ./bin/kcli produce --help
Produce messages to a Kafka topic

Usage:
  kcli produce [flags]

Aliases:
  produce, p

Flags:
  -b, --bootstrap-servers strings   [REQUIRED] Kafka bootstrap servers, split by ',' (e.g., 'localhost:9092,localhost:9093' - comma separated)
  -f, --file string                 Read message payload from file
  -h, --help                        help for produce
  -k, --key string                  Message key, if this option empty, the key will be generated automatically
      --password string             Password for authentication
  -p, --payload string              Message payload
      --topic string                [REQUIRED] Kafka topic to produce messages to
      --username string             Username for authentication
      --with-timeout int            Timeout for producing message in second, default 5s (default 5)

$ ./bin/kcli produce --bootstrap-servers='localhost:8097,localhost:8098,localhost:8099' --topic='be-application-test-01' --payload='{"test1":"data","test2":2}'
```