package common

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
)

// Serializer defines the interface for serialization and deserialization.
type Serializer interface {
	Serialize(topic string, value interface{}) ([]byte, error)
	Deserialize(topic string, data []byte) (interface{}, error)
}

// AvroSerializer implements the Serializer interface using Avro.
type AvroSerializer struct {
	serializer   *avro.GenericSerializer
	deserializer *avro.GenericDeserializer
}

// NewAvroSerializer creates a new AvroSerializer.
func NewAvroSerializer(schemaRegistryURL string) (*AvroSerializer, error) {
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		return nil, err
	}

	serializer, err := avro.NewGenericSerializer(client, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		return nil, err
	}

	deserializer, err := avro.NewGenericDeserializer(client, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		return nil, err
	}
	return &AvroSerializer{
		serializer:   serializer,
		deserializer: deserializer,
	}, nil
}

// Serialize encodes the value using Avro.
func (a *AvroSerializer) Serialize(topic string, value interface{}) ([]byte, error) {
	return a.serializer.Serialize(topic, value)
}

// Deserialize decodes the data using Avro.
func (a *AvroSerializer) Deserialize(topic string, data []byte) (interface{}, error) {
	return a.deserializer.Deserialize(topic, data)
}

type JsonSerializer struct {
	serializer   *jsonschema.Serializer
	deserializer *jsonschema.Deserializer
}

func NewJsonSerializer(schemaRegistryURL string) (*JsonSerializer, error) {
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		return nil, err
	}

	serializer, err := jsonschema.NewSerializer(client, serde.ValueSerde, jsonschema.NewSerializerConfig())
	if err != nil {
		return nil, err
	}

	deserializer, err := jsonschema.NewDeserializer(client, serde.ValueSerde, jsonschema.NewDeserializerConfig())
	if err != nil {
		return nil, err
	}

	return &JsonSerializer{
		serializer:   serializer,
		deserializer: deserializer,
	}, nil
}

func (j *JsonSerializer) Serialize(topic string, value interface{}) ([]byte, error) {
	if j.serializer == nil {
		log.Println("Serializer is nil")
		return nil, fmt.Errorf("serializer is nil")
	}

	return j.serializer.Serialize(topic, value)
}

func (j *JsonSerializer) Deserialize(topic string, data []byte) (interface{}, error) {
	if j.deserializer == nil {
		log.Println("Deserializer is nil")
		return nil, fmt.Errorf("deserializer is nil")
	}
	return j.deserializer.Deserialize(topic, data)
}
