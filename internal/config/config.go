package config

import (
	"sync"

	"github.com/mata-elang-stable/event-stream-aggr/internal/logger"
)

type Config struct {
	// SchemaRegistryUrl is the schema registry URL.
	SchemaRegistryUrl string `mapstructure:"schema_registry_url"`

	// KafkaBrokers is the Kafka broker to connect to.
	KafkaBrokers string `mapstructure:"kafka_brokers"`

	// InputKafkaTopic is the Kafka topic.
	InputKafkaTopic string `mapstructure:"input_kafka_topic"`

	// OutputKafkaTopic is the Kafka topic.
	OutputKafkaTopic string `mapstructure:"output_kafka_topic"`

	// MaxWorkers is the maximum number of workers. 0 means auto.
	MaxWorkers int `mapstructure:"max_workers"`

	// LogInterval is the interval to log the status.
	LogInterval int `mapstructure:"log_interval"`

	// VerboseCount is the verbose level.
	VerboseCount int `mapstructure:"verbose"`

	// SecurityProtocol is the security protocol to use.
	SecurityProtocol string `mapstructure:"security_protocol"`

	// PathToCA is the path to the CA certificate file.
	PathToCA string `mapstructure:"path_to_ca"`

	// PathToClientKeystore is the path to a PKCS#12 keystore that contains client certificate and key
	PathToClientKeystore string `mapstructure:"path_to_client_keystore"`

	// ClientKeystorePassword is the password for the keystore file
	ClientKeystorePassword string `mapstructure:"client_keystore_password"`
}

var log = logger.GetLogger()

var instance *Config
var once sync.Once

func GetConfig() *Config {
	once.Do(func() {
		instance = &Config{}
	})

	return instance
}

func (c *Config) SetupLogging() {
	switch instance.VerboseCount {
	case 0:
		log.SetLevel(logger.InfoLevel)
	case 1:
		log.SetLevel(logger.DebugLevel)
	default:
		log.SetLevel(logger.TraceLevel)
	}
	log.WithFields(logger.Fields{
		"LOG_LEVEL": log.GetLevel().String(),
	}).Infoln("Logging level set.")
}

func (c *Config) PrintAllConfigs() {
	log.Printf("SchemaRegistryUrl: %s\n", c.SchemaRegistryUrl)
	log.Printf("KafkaBrokers: %s\n", c.KafkaBrokers)
	log.Printf("InputKafkaTopic: %s\n", c.InputKafkaTopic)
	log.Printf("OutputKafkaTopic: %s\n", c.OutputKafkaTopic)
	log.Printf("MaxWorkers: %d\n", c.MaxWorkers)
	log.Printf("VerboseCount: %d\n", c.VerboseCount)
	log.Printf("SecurityProtocol: %s\n", c.SecurityProtocol)
	log.Printf("PathToCA: %s\n", c.PathToCA)
	log.Printf("PathToClientKeystore: %s\n", c.PathToClientKeystore)
	if c.ClientKeystorePassword != "" {
		log.Printf("ClientKeystorePassword: (set)\n")
	} else {
		log.Printf("ClientKeystorePassword: (not set)\n")
	}
}
