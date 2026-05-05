package kafka_client

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/mata-elang-stable/event-stream-aggr/internal/logger"
)

func MustNewConsumer(servers string, topic string, securityProtocol string, brokerCaPath string, clientKeystorePath string, clientKeystorePassword string) (*kafka.Consumer, error) {
	// Normalize security protocol and assemble config map conditionally so SSL is optional.
	sp := strings.ToUpper(strings.TrimSpace(securityProtocol))

	log := logger.GetLogger()

	if sp != "PLAINTEXT" {
		if brokerCaPath == "" {
			log.WithFields(logger.Fields{
				"security_protocol": securityProtocol,
				"broker_ca_path":    brokerCaPath,
			}).Warnln("security protocol configured but CA path not provided; falling back to PLAINTEXT")
			sp = "PLAINTEXT"
		}
	}

	cfg := &kafka.ConfigMap{
		"bootstrap.servers":  servers,
		"group.id":           "mataelang-dc-dashboard-command",
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	}

	if sp == "" {
		sp = "PLAINTEXT"
	}
	(*cfg)["security.protocol"] = sp

	if sp != "PLAINTEXT" {
		if brokerCaPath != "" {
			(*cfg)["ssl.ca.location"] = brokerCaPath
		}
		// Use keystore (PKCS#12/JKS) if provided
		if clientKeystorePath != "" {
			(*cfg)["ssl.keystore.location"] = clientKeystorePath
			if clientKeystorePassword != "" {
				(*cfg)["ssl.keystore.password"] = clientKeystorePassword
			}
		}
	}

	consumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		return nil, err
	}

	if err := consumer.Subscribe(topic, nil); err != nil {
		return nil, err
	}

	return consumer, nil
}
