package main

import (
	"fmt"
	"os"

	"github.com/mata-elang-stable/event-stream-aggr/internal/config"
	"github.com/mata-elang-stable/event-stream-aggr/internal/logger"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	appVersion = "dev"
	appCommit  = "none"
	appLicense = "MIT"
)

var log = logger.GetLogger()

var rootCmd = &cobra.Command{
	Use:   "sensor_events_handler_aggr",
	Short: "sensor_events_handler_aggr is a Kafka consumer that reads sensor events and produces alerts",
	Args:  cobra.NoArgs,
	Run:   runApp,
}

func init() {
	// Read configuration from .env file in the current directory
	// viper.SetConfigFile("./.env")
	viper.SetConfigName(".env")
	viper.SetConfigType("env")
	viper.AddConfigPath(".")

	err := viper.ReadInConfig()
	if err != nil {
		log.WithField("error", err).Infoln("Failed to read configuration file")
	}

	viper.AutomaticEnv()

	conf := config.GetConfig()

	viper.SetDefault("kafka_brokers", "localhost:9092")
	viper.SetDefault("input_kafka_topic", "sensor_events")
	viper.SetDefault("schema_registry_url", "http://localhost:8081")
	viper.SetDefault("output_kafka_topic", "snort_alerts")
	viper.SetDefault("max_workers", 0)
	viper.SetDefault("log_interval", 10)
	viper.SetDefault("verbose", 0)
	viper.SetDefault("security_protocol", "PLAINTEXT")
	viper.SetDefault("path_to_ca", "")
	viper.SetDefault("path_to_client_keystore", "")
	viper.SetDefault("client_keystore_password", "")

	if err := viper.Unmarshal(&conf); err != nil {
		log.WithField("error", err).Fatalln("Failed to unmarshal configuration.")
	}

	flags := rootCmd.PersistentFlags()

	flags.StringVar(&conf.KafkaBrokers, "kafka-brokers", conf.KafkaBrokers, "")
	flags.StringVar(&conf.InputKafkaTopic, "input-topic", conf.InputKafkaTopic, "")
	flags.StringVar(&conf.SchemaRegistryUrl, "schema-registry-url", conf.SchemaRegistryUrl, "")
	flags.StringVar(&conf.OutputKafkaTopic, "output-topic", conf.OutputKafkaTopic, "")
	flags.StringVar(&conf.PathToClientKeystore, "path-to-client-keystore", conf.PathToClientKeystore, "Path to the client PKCS#12 keystore file for mTLS")
	flags.StringVar(&conf.ClientKeystorePassword, "client-keystore-password", conf.ClientKeystorePassword, "Password for the client PKCS#12 keystore")
	// Keystore type no longer configurable: PKCS#12 is used for librdkafka by default
	flags.CountVarP(&conf.VerboseCount, "verbose", "v", "Increase verbosity of the output.")

	if err := viper.BindPFlags(flags); err != nil {
		log.WithField("error", err).Fatalln("Failed to bind flags.")
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
