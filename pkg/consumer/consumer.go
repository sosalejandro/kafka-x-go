package consumer

import (
	"context"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sosalejandro/kafka-x-go/pkg/common"
	"github.com/sosalejandro/kafka-x-go/pkg/producer"
)

// MessageHandler is the interface that defines the contract for handling messages.
type MessageHandler interface {
	HandleMessage(ctx context.Context, key interface{}, value interface{}, producer *producer.Producer) error
}

// HandlerRegistry maps topics to MessageHandler instances.
type HandlerRegistry struct {
	handlers map[string]MessageHandler
}

// NewHandlerRegistry initializes an empty HandlerRegistry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[string]MessageHandler),
	}
}

// RegisterHandler associates a handler with a topic.
func (r *HandlerRegistry) RegisterHandler(topic string, handler MessageHandler) {
	r.handlers[topic] = handler
}

// GetHandler retrieves the handler for the given topic.
func (r *HandlerRegistry) GetHandler(topic string) (MessageHandler, bool) {
	handler, exists := r.handlers[topic]
	return handler, exists
}

// Consumer encapsulates Kafka consumer logic.
type Consumer struct {
	consumer     *kafka.Consumer
	deserializer common.Serializer
	registry     *HandlerRegistry
	producer     *producer.Producer
}

// NewConsumer creates a new Kafka consumer with a handler registry.
func NewConsumer(config common.KafkaConfig, deserializer common.Serializer, registry *HandlerRegistry, producer *producer.Producer) (*Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": config.BootstrapServers,
		"group.id":          config.GroupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, err
	}
	return &Consumer{
		consumer:     c,
		deserializer: deserializer,
		registry:     registry,
		producer:     producer,
	}, nil
}

// Start begins consuming messages from the specified topics.
func (c *Consumer) Start(ctx context.Context, topics []string) error {
	if err := c.consumer.SubscribeTopics(topics, nil); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down consumer...")
			return nil
		default:
			msg, err := c.consumer.ReadMessage(-1)
			if err != nil {
				log.Printf("Consumer error: %v", err)
				continue
			}

			topic := *msg.TopicPartition.Topic
			handler, exists := c.registry.GetHandler(topic)
			if !exists {
				log.Printf("No handler registered for topic: %s", topic)
				continue
			}

			key := msg.Key
			value, err := c.deserializer.Deserialize(topic, msg.Value)
			if err != nil {
				log.Printf("Deserialization error for topic %s: %v", topic, err)
				continue
			}

			if err := handler.HandleMessage(ctx, key, value, c.producer); err != nil {
				log.Printf("Error handling message from topic %s: %v", topic, err)
			}
		}
	}
}
