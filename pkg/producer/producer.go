package producer

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sosalejandro/kafka-x-go/pkg/common"
)

// Producer encapsulates Kafka producer logic.
type Producer struct {
	producer   *kafka.Producer
	serializer common.Serializer
}

// NewProducer creates a new Kafka producer.
func NewProducer(config common.KafkaConfig, serializer common.Serializer) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.BootstrapServers,
	})
	if err != nil {
		return nil, err
	}
	return &Producer{producer: p, serializer: serializer}, nil
}

// Produce sends a message to the specified topic.
func (p *Producer) Produce(topic string, key interface{}, value interface{}) error {
	serializedValue, err := p.serializer.Serialize(topic, value)
	if err != nil {
		return err
	}

	serializedKey, err := p.serializer.Serialize(topic, key)
	if err != nil {
		return err
	}
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            serializedKey,
		Value:          serializedValue,
		Headers:        []kafka.Header{{Key: string(serializedKey), Value: serializedKey}},
	}, deliveryChan)

	if err != nil {
		log.Printf("Failed to produce message: %v", err)
		return err
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v", m.TopicPartition.Error)
		return m.TopicPartition.Error
	}

	log.Printf("Produced message: key=%s, value=%s", key, value)

	return nil
}

// Close cleans up the producer.
func (p *Producer) Close() {
	p.producer.Close()
}

// // MessageHandler defines the interface for processing consumed messages.
// type MessageHandler interface {
// 	HandleMessage(ctx context.Context, key, value []byte, producer *Producer) error
// }

// // Consumer encapsulates Kafka consumer logic.
// type Consumer struct {
// 	consumer     *kafka.Consumer
// 	deserializer *serialization.Deserializer
// 	handler      MessageHandler
// 	producer     *Producer
// }

// // NewConsumer creates a new Kafka consumer with a message handler.
// func NewConsumer(config common.KafkaConfig, handler MessageHandler, producer *Producer) (*Consumer, error) {
// 	c, err := kafka.NewConsumer(&kafka.ConfigMap{
// 		"bootstrap.servers": config.BootstrapServers,
// 		"group.id":          config.GroupID,
// 		"auto.offset.reset": "earliest",
// 	})
// 	if err != nil {
// 		return nil, err
// 	}

// 	deserializer, err := serialization.NewAvroDeserializer(config.SchemaRegistryURL, nil)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &Consumer{consumer: c, deserializer: deserializer, handler: handler, producer: producer}, nil
// }

// // Start begins consuming messages from the specified topics.
// func (c *Consumer) Start(ctx context.Context, topics []string) error {
// 	if err := c.consumer.SubscribeTopics(topics, nil); err != nil {
// 		return err
// 	}

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			log.Println("Shutting down consumer...")
// 			return nil
// 		default:
// 			msg, err := c.consumer.ReadMessage(-1)
// 			if err != nil {
// 				log.Printf("Consumer error: %v", err)
// 				continue
// 			}

// 			key := msg.Key
// 			value, err := c.deserializer.Deserialize(msg.Value)
// 			if err != nil {
// 				log.Printf("Deserialization error: %v", err)
// 				continue
// 			}

// 			if err := c.handler.HandleMessage(ctx, key, value, c.producer); err != nil {
// 				log.Printf("Message handling error: %v", err)
// 			}
// 		}
// 	}
// }

// // Close cleans up the consumer.
// func (c *Consumer) Close() {
// 	c.consumer.Close()
// }

// // ExampleHandler is an example implementation of MessageHandler.
// type ExampleHandler struct{}

// // HandleMessage processes a consumed message and optionally produces new messages.
// func (h *ExampleHandler) HandleMessage(ctx context.Context, key, value []byte, producer *Producer) error {
// 	log.Printf("Consumed message: key=%s, value=%s", key, value)

// 	// Example logic: produce a new message based on the consumed one.
// 	newValue := append(value, []byte(" processed")...)
// 	if err := producer.Produce("processed-topic", key, newValue); err != nil {
// 		return err
// 	}

// 	return nil
// }
