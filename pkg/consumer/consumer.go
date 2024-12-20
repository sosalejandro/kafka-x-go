package consumer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sosalejandro/kafka-x-go/pkg/common"
	"github.com/sosalejandro/kafka-x-go/pkg/common/retry"
	"github.com/sosalejandro/kafka-x-go/pkg/metrics"
	"github.com/sosalejandro/kafka-x-go/pkg/observability"
	"github.com/sosalejandro/kafka-x-go/pkg/producer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// MessageHandler is the interface that defines the contract for handling messages.
type MessageHandler[T any] interface {
	HandleMessage(ctx context.Context, key interface{}, value T, producer *producer.Producer) error
	GetMessageType() reflect.Type
}

// HandlerRegistry maps topics to MessageHandler instances.
type HandlerRegistry struct {
	handlers map[string]MessageHandler[any]
}

// NewHandlerRegistry initializes an empty HandlerRegistry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[string]MessageHandler[any]),
	}
}

// RegisterHandler associates a handler with a topic.
func (r *HandlerRegistry) RegisterHandler(topic string, handler MessageHandler[any]) {
	r.handlers[topic] = handler
}

// GetHandler retrieves the handler for the given topic.
func (r *HandlerRegistry) GetHandler(topic string) (MessageHandler[any], bool) {
	handler, exists := r.handlers[topic]
	return handler, exists
}

// Consumer encapsulates Kafka consumer logic with concurrency and retry control.
type Consumer struct {
	consumer      *kafka.Consumer
	deserializer  common.Serializer
	registry      *HandlerRegistry
	producer      *producer.Producer
	retryStrategy retry.RetryStrategy
	logger        *zap.Logger
	DLQTopic      string
}

// NewConsumer creates a new Kafka consumer with a handler registry, worker pool, retry strategy, and logger.
func NewConsumer(
	config common.KafkaConfig,
	deserializer common.Serializer,
	registry *HandlerRegistry,
	producer *producer.Producer,
	strategy retry.RetryStrategy,
	logger *zap.Logger,
	dlqTopic string, // Added parameter
) (*Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  config.BootstrapServers,
		"group.id":           config.GroupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false, // Ensure auto-commit is disabled
	})

	if err != nil {
		logger.Error("Error creating Kafka consumer", zap.Error(err))
		return nil, err
	}
	return &Consumer{
		consumer:      c,
		deserializer:  deserializer,
		registry:      registry,
		producer:      producer,
		retryStrategy: strategy,
		logger:        logger,
		DLQTopic:      dlqTopic, // Set DLQTopic
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
			c.logger.Info("Context cancelled. Closing consumer...")
			return nil
		default:
			ev := c.consumer.Poll(10)
			if ev == nil {
				continue
			}

			c.logger.Info("Received event", zap.Any("event", ev))

			// Start open telemetry span
			ctx, span := otel.Tracer("").Start(ctx, "Consumer.Start")
			defer span.End()

			switch e := ev.(type) {
			case *kafka.Message:
				// observability: Add event to the span
				span.AddEvent("Received message")

				topic := *e.TopicPartition.Topic
				handler, exists := c.registry.GetHandler(topic)

				spanAttr := attribute.NewSet(
					attribute.String("topic", topic),
					attribute.String("key", string(e.Key)),
				)

				// observability: Add attributes to the span
				span.SetAttributes(
					spanAttr.ToSlice()...,
				)

				metrics.GetReceivedMessagesCounter().Add(ctx, 1, metric.WithAttributes(
					attribute.String("topic", topic),
				))

				if !exists {
					// observability: Record error in the span
					span.RecordError(errors.New("no handler registered for topic"), trace.WithAttributes(
						attribute.String("topic", topic),
						attribute.String("timestamp", time.Now().String()),
					))

					span.SetStatus(codes.Error, "No handler registered for topic")

					c.logger.Error(
						"No handler registered for topic",
						zap.Object("error", zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
							enc.AddString("topic", topic)
							enc.AddTime("timestamp", time.Now())
							return nil
						}),
						),
					)

					metrics.GetIgnoredEventsCounter().Add(ctx, 1, metric.WithAttributeSet(
						spanAttr,
					))
					continue
				}

				c.processMessage(ctx, e, handler)
			case kafka.Error:
				c.logger.Error("Kafka error", zap.Error(e))
				metrics.GetFailedCommitsCounter().Add(ctx, 1)
			default:
				c.logger.Error("Ignored event", zap.Any("event", e))
				metrics.GetIgnoredEventsCounter().Add(ctx, 1)
			}
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg *kafka.Message, handler MessageHandler[any]) {
	carrier := observability.NewKafkaHeadersCarrier(msg.Headers)
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

	// Start a new span
	ctx, span := otel.Tracer("").Start(ctx, "Consumer.processMessage")
	defer span.End()

	topic := *msg.TopicPartition.Topic
	key := msg.Key
	valueType := handler.GetMessageType()

	otelAttrs := attribute.NewSet(
		attribute.String("topic", topic),
		attribute.String("key", string(key)),
		attribute.String("message_type", valueType.String()),
	)

	// observability: Add attributes to the span
	span.SetAttributes(
		otelAttrs.ToSlice()...,
	)

	value := reflect.New(valueType).Interface()
	err := c.deserializer.DeserializeInto(topic, msg.Value, &value)
	if err != nil {
		// observability: Record error in the span
		span.RecordError(err)
		span.SetStatus(codes.Error, "Error deserializing message")
		metrics.GetDeserializationErrorsCounter().Add(ctx, 1, metric.WithAttributeSet(
			otelAttrs,
		))
		c.logger.Error("Error deserializing message", zap.Error(err), zap.String("topic", topic))

		return
	}

	var attempt int

	for {
		attempt++
		// observability: Add event for each attempt
		retryingAttr := attribute.NewSet(
			attribute.Int("attempt", attempt),
		)
		span.AddEvent("Processing message", trace.WithAttributes(
			retryingAttr.ToSlice()...,
		))

		err = handler.HandleMessage(ctx, key, value, c.producer)

		if err == nil {
			// Success
			// observability: Add event for successful message processing
			span.AddEvent("Successfully processed message", trace.WithAttributes(
				attribute.String("topic", topic),
			))
			c.logger.Info("Successfully processed message", zap.Any("message", value), zap.String("topic", topic))
			metrics.GetSuccessfulMessagesCounter().Add(
				ctx,
				1,
				metric.WithAttributeSet(retryingAttr),
				metric.WithAttributeSet(otelAttrs),
			)

			break
		}

		if attempt >= c.retryStrategy.MaxRetries() { // Updated to use MaxRetries()
			// Exceeded max retries, send to DLQ if configured
			// observability: Add event for exceeded max retries
			maxRetriesAttr := attribute.Int("max retries", c.retryStrategy.MaxRetries())
			span.AddEvent("Exceeded max retries", trace.WithAttributes(
				maxRetriesAttr,
			))
			c.logger.Warn("Exceeded max retries. Sending message to DLQ", zap.String("topic", topic))
			metrics.GetUnsuccessfulMessagesCounter().Add(
				ctx,
				1,
				metric.WithAttributes(maxRetriesAttr),
				metric.WithAttributeSet(retryingAttr),
				metric.WithAttributeSet(otelAttrs),
			)

			// observability: Send message to DLQ
			span.AddEvent("Sending message to DLQ")
			if err := c.sendToDLQ(ctx, c.DLQTopic, msg, err); err != nil {
				// observability: Record error in the span
				span.RecordError(err)
				span.SetStatus(codes.Error, "Error sending message to DLQ")
				c.logger.Error("Error sending message to DLQ", zap.Error(err), zap.String("topic", topic))
			}
			break
		}

		metrics.GetUnsuccessfulRetriesCounter().Add(
			ctx,
			1,
			metric.WithAttributeSet(retryingAttr),
			metric.WithAttributeSet(otelAttrs),
		)

		// Wait before retrying using the retry strategy
		backoff := c.retryStrategy.NextInterval(attempt)
		c.logger.Error("Error processing message. Retrying...", zap.Error(err), zap.Duration("backoff", backoff), zap.String("topic", topic))

		// Use a timer instead of ticker to wait for the backoff duration
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			// observability: Record error in the span
			span.RecordError(ctx.Err())
			span.SetStatus(codes.Error, "Context cancelled before retrying")
			c.logger.Error("Context cancelled before retrying", zap.Error(ctx.Err()), zap.String("topic", topic))
			<-timer.C // Ensure the timer completes to prevent goroutine leak
		case <-timer.C:
			// Proceed to next retry attempt
		}
		timer.Stop()
	}

	// Mark message as processed by committing the message offset
	// observability: Add event for committing message offset
	span.AddEvent("Committing message offset")
	_, err = c.consumer.CommitMessage(msg)
	span.AddEvent("Message offset committed")
	if err != nil {
		// observability: Record error in the span
		span.RecordError(err)
		span.SetStatus(codes.Error, "Error committing message offset")
		c.logger.Error("Error committing message offset", zap.Error(err), zap.String("topic", topic))
		// TODO: Optionally handle the commit error (e.g., retry committing)
	}
}

func (c *Consumer) Close() {
	c.consumer.Close()
}

func (c *Consumer) sendToDLQ(ctx context.Context, dlqTopic string, msg *kafka.Message, err error) error {
	if dlqTopic == "" {
		// No DLQ configured, skip sending
		return nil
	}

	// Start a new span from the context
	ctx, span := otel.Tracer("").Start(ctx, fmt.Sprintf("Consumer.sendToDLQ.%s", dlqTopic))
	defer span.End()

	otelAttrs := attribute.NewSet(
		attribute.String("topic", *msg.TopicPartition.Topic),
		attribute.String("key", string(msg.Key)),
		attribute.String("dql_queue", dlqTopic),
	)

	// Generate a descriptive message to send to the DLQ
	dlqMsg := map[string]interface{}{
		"error": err.Error(),
		"topic": *msg.TopicPartition.Topic,
		"key":   string(msg.Key),
		"value": string(msg.Value),
	}

	// Serialize the message
	serialized, err := c.deserializer.Serialize("dlq", dlqMsg)
	if err != nil {
		c.logger.Error("Error serializing message for DLQ", zap.Error(err), zap.String("topic", dlqMsg["topic"].(string)))
		span.RecordError(err)
		span.SetStatus(codes.Error, "Error serializing message for DLQ")
		metrics.GetSerializationErrorsCounter().Add(
			ctx,
			1,
			metric.WithAttributeSet(otelAttrs),
		)
		return err
	}

	// Check if context is still active before producing
	select {
	case <-ctx.Done():
		span.RecordError(ctx.Err())
		span.SetStatus(codes.Error, "Context cancelled before sending message to DLQ")
		c.logger.Error("Context cancelled before sending message to DLQ", zap.Error(ctx.Err()))
		return ctx.Err()
	default:
		// Proceed to produce the message to DLQ
	}

	// Produce the message to the specified DLQ topic
	err = c.producer.Produce(ctx, dlqTopic, msg.Key, serialized)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Error sending message to DLQ")
		c.logger.Error("Error sending message to DLQ", zap.Error(err), zap.String("topic", dlqMsg["topic"].(string)))
		return err
	}

	c.logger.Info("Sent message to DLQ", zap.Any("message", dlqMsg))

	return nil
}
