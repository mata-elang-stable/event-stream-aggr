package main

import (
	"context"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/mata-elang-stable/event-stream-aggr/internal/app"
	"github.com/mata-elang-stable/event-stream-aggr/internal/config"
	"github.com/mata-elang-stable/event-stream-aggr/internal/kafka_client"
	"github.com/mata-elang-stable/event-stream-aggr/internal/schema"
	"github.com/spf13/cobra"
)

func runApp(cmd *cobra.Command, args []string) {
	// Load the configuration

	log.Infof("Configuring the app\n")
	conf := config.GetConfig()
	conf.SetupLogging()

	conf.PrintAllConfigs()

	mainContext, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancel()

	// if termination signal received 2 times, exit immediately
	go func() {
		<-mainContext.Done()

		log.Infof("Received termination signal\n")

		secondSignal, cancel2 := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		cancel()

		<-secondSignal.Done()

		log.Infof("Received termination signal 2 times\n")

		cancel2()

		os.Exit(1)
	}()

	log.Infof("Configuring the kafka client\n")
	kafkaConsumer, err := kafka_client.MustNewConsumer(conf.KafkaBrokers, conf.InputKafkaTopic)
	if err != nil {
		log.Fatalf("Failed to create kafka consumer: %v", err)
	}

	kafkaProducer, err := kafka_client.MustNewProducer(conf.KafkaBrokers)
	if err != nil {
		log.Fatalf("Failed to create kafka producer: %v", err)
	}

	log.Infof("Configuring the schema registry client\n")
	schemaRegistryClient := schema.MustNewSchemaRegistryClient(conf.SchemaRegistryUrl)

	deserializer := schema.MustNewProtobufDeserializer(schemaRegistryClient)
	serializer := schema.MustNewAvroSerializer(schemaRegistryClient)

	numOfWorkers := int(max(getCPUQuota(), 1) * 1024)

	if conf.MaxWorkers != 0 {
		numOfWorkers = conf.MaxWorkers
	}

	log.Infof("Setting max workers to %d\n", numOfWorkers)

	mainApp := app.NewApp(
		kafkaConsumer,
		deserializer,
		kafkaProducer,
		serializer,
		conf.InputKafkaTopic,
		conf.OutputKafkaTopic,
		numOfWorkers,
		conf.LogInterval,
	)

	if err := mainApp.Run(mainContext); err != nil {
		log.Errorf("Error running the app: %v\n", err)
	}
}

func getCPUQuota() float64 {
	quotaPath := "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
	periodPath := "/sys/fs/cgroup/cpu/cpu.cfs_period_us"

	quotaBytes, err1 := os.ReadFile(quotaPath)
	periodBytes, err2 := os.ReadFile(periodPath)

	if err1 != nil || err2 != nil {
		return float64(runtime.NumCPU()) // Fallback to physical CPU count
	}

	quota, err1 := strconv.Atoi(strings.TrimSpace(string(quotaBytes)))
	period, err2 := strconv.Atoi(strings.TrimSpace(string(periodBytes)))

	if err1 != nil || err2 != nil || quota < 0 {
		return float64(runtime.NumCPU()) // If unlimited, fallback
	}

	return float64(quota) / float64(period) // Calculate CPU limit
}
