package kafka_client

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func MustNewProducer(servers string) (*kafka.Producer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        servers,
		"enable.idempotence":       true,
		"linger.ms":                10,
		"batch.size":               65536,
		"acks":                     "all",
		"message.send.max.retries": 5,
		"retry.backoff.ms":         100,
		"socket.keepalive.enable":  true,
		"go.batch.producer":        true,

		// disabled by issue:  https://github.com/confluentinc/confluent-kafka-python/issues/84
		//"delivery.report.only.error": true,
	})
	if err != nil {
		return nil, err
	}

	return producer, nil
}
