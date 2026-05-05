package kafka_client

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/mata-elang-stable/event-stream-aggr/internal/logger"
)

func MustNewProducer(servers string, securityProtocol string, pathToCA string, clientKeystorePath string, clientKeystorePassword string) (*kafka.Producer, error) {
	sp := strings.ToUpper(strings.TrimSpace(securityProtocol))
	if sp == "" {
		sp = "PLAINTEXT"
	}

	cfg := &kafka.ConfigMap{
		"bootstrap.servers":        servers,
		"enable.idempotence":       true,
		"linger.ms":                10,
		"batch.size":               65536,
		"acks":                     "all",
		"message.send.max.retries": 5,
		"retry.backoff.ms":         100,
		"socket.keepalive.enable":  true,
		"go.batch.producer":        true,
	}

	// Warn and fallback to PLAINTEXT if security protocol indicates SSL but no CA provided
	log := logger.GetLogger()
	if sp != "PLAINTEXT" && pathToCA == "" {
		log.WithFields(logger.Fields{"security_protocol": securityProtocol}).Warnln("security protocol configured but CA path not provided; falling back to PLAINTEXT")
		sp = "PLAINTEXT"
	}
	(*cfg)["security.protocol"] = sp
	if sp != "PLAINTEXT" {
		if pathToCA != "" {
			(*cfg)["ssl.ca.location"] = pathToCA
		}
		if clientKeystorePath != "" {
			(*cfg)["ssl.keystore.location"] = clientKeystorePath
			if clientKeystorePassword != "" {
				(*cfg)["ssl.keystore.password"] = clientKeystorePassword
			}
		}
	}

	// Disabled by issue: https://github.com/confluentinc/confluent-kafka-python/issues/84
	// The equivalent option can be set on cfg if we ever need to.
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		return nil, err
	}

	return producer, nil
}
