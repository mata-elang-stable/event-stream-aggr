package schema

import (
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/fadhilyori/sensor_events_handler_aggr/internal/logger"
	"github.com/fadhilyori/sensor_events_handler_aggr/internal/pb"
)

var log = logger.GetLogger()

func MustNewSchemaRegistryClient(schemaRegistryUrl string) schemaregistry.Client {
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryUrl))
	if err != nil {
		log.Fatalf("Failed to create schema registry client: %s\n", err)
	}

	return client
}

func MustNewProtobufDeserializer(schemaRegistryClient schemaregistry.Client) *protobuf.Deserializer {
	deserializer, err := protobuf.NewDeserializer(schemaRegistryClient, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		log.Fatalf("Failed to create deserializer: %s\n", err)
	}

	if err := deserializer.ProtoRegistry.RegisterMessage((&pb.SensorEvent{}).ProtoReflect().Type()); err != nil {
		log.Fatalf("Error registering ProtoBuf message type: %v\n", err)
	}

	return deserializer
}

func MustNewAvroSerializer(schemaRegistryClient schemaregistry.Client) *avro.GenericSerializer {
	serializer, err := avro.NewGenericSerializer(schemaRegistryClient, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		log.Fatalf("Failed to create serializer: %s\n", err)
	}

	return serializer
}
