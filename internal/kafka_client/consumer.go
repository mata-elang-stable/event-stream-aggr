package kafka_client

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

func MustNewConsumer(servers string, topic string) (*kafka.Consumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  servers,
		"group.id":           "mataelang-dc-dashboard-command",
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}

	if err := consumer.Subscribe(topic, nil); err != nil {
		return nil, err
	}

	return consumer, nil
}
