package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sosalejandro/kafka-x-go/pkg/common"
	"github.com/sosalejandro/kafka-x-go/pkg/common/retry"
	"github.com/sosalejandro/kafka-x-go/pkg/metrics"
	"github.com/sosalejandro/kafka-x-go/pkg/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Producer encapsulates Kafka producer logic.
type Producer struct {
	producer      *kafka.Producer
	serializer    common.Serializer
	logger        *zap.Logger
	retryStrategy retry.RetryStrategy
}

// NewProducer creates a new Kafka producer.
func NewProducer(config common.KafkaConfig, serializer common.Serializer, logger *zap.Logger, strategy retry.RetryStrategy) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.BootstrapServers,
	})
	if err != nil {
		logger.Error("Failed to create Kafka producer", zap.Error(err))
		return nil, err
	}
	return &Producer{
		producer:      p,
		serializer:    serializer,
		logger:        logger,
		retryStrategy: strategy,
	}, nil
}

// Produce sends a message to the specified topic with retry logic.
func (p *Producer) Produce(ctx context.Context, topic string, key interface{}, value interface{}) error {
	// Start span for producing message
	ctx, span := otel.Tracer(fmt.Sprintf("%s-producer", topic)).Start(ctx, "Producer.Produce")
	defer span.End()

	otelAttrs := attribute.NewSet(
		attribute.String("topic", topic),
		attribute.String("key", key.(string)),
	)

	// observability: Add event for producing message
	span.AddEvent("Producing message", trace.WithAttributes(
		otelAttrs.ToSlice()...,
	))

	serializedValue, err := p.serializer.Serialize(topic, value)
	if err != nil {
		// observability: record error
		metrics.GetSerializationErrorsCounter().Add(ctx, 1, metric.WithAttributeSet(otelAttrs))
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to serialize message value")
		p.logger.Error("Failed to serialize message value", zap.Error(err), zap.String("topic", topic))
		return err
	}

	// observability: Add event for serialized message value
	span.AddEvent("Serialized message value", trace.WithAttributes(
		attribute.String("serialized_value", string(serializedValue)),
	))

	serializedKey, err := p.serializer.Serialize(topic, key)
	if err != nil {
		// observability: record error
		metrics.GetSerializationErrorsCounter().Add(ctx, 1, metric.WithAttributeSet(otelAttrs))
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to serialize message key")
		p.logger.Error("Failed to serialize message key", zap.Error(err), zap.String("topic", topic))
		return err
	}

	// observability: Add event for serialized message key
	span.AddEvent("Serialized message key", trace.WithAttributes(
		attribute.String("serialized_key", string(serializedKey)),
	))

	var attempt int
	for {
		// observability: Add event for each attempt
		span.AddEvent("Producing message attempt", trace.WithAttributes(
			attribute.Int("attempt", attempt),
		))
		attempt++
		deliveryChan := make(chan kafka.Event, 1)
		defer close(deliveryChan)

		// Create a headers carrier and inject the context
		headers := make([]kafka.Header, 0)

		for k, v := range extractContextValues(ctx) {
			headers = append(headers, kafka.Header{
				Key:   k,
				Value: []byte(v),
			})
		}

		// Wrap headers in KafkaHeadersCarrier using a pointer
		carrier := observability.NewKafkaHeadersCarrier(headers)
		carrier.Set(string(serializedKey), string(serializedKey))
		otel.GetTextMapPropagator().Inject(ctx, carrier)

		err = p.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            serializedKey,
			Value:          serializedValue,
			Headers:        carrier.GetHeaders(),
		}, deliveryChan)

		spanAttr := attribute.NewSet(
			attribute.String("topic", topic),
			attribute.String("key", string(serializedKey)),
			attribute.Int("attempt", attempt),
		)

		if err == nil {
			select {
			case e := <-deliveryChan:
				m := e.(*kafka.Message)
				if m.TopicPartition.Error != nil {
					// observability: record error
					span.RecordError(m.TopicPartition.Error)
					span.SetStatus(codes.Error, "Failed to deliver message")
					p.logger.Error("Delivery failed", zap.Error(m.TopicPartition.Error), zap.String("topic", topic))

					err = m.TopicPartition.Error

					metrics.GetUnsuccessfulMessagesCounter().Add(ctx, 1, metric.WithAttributeSet(
						spanAttr,
					))
				} else {
					// observability: add event
					metrics.GetSuccessfulMessagesCounter().Add(ctx, 1,
						metric.WithAttributeSet(spanAttr),
					)
					span.AddEvent("Message delivered", trace.WithAttributes(
						spanAttr.ToSlice()...,
					))
					p.logger.Info("Produced message successfully", zap.ByteString("key", serializedKey), zap.ByteString("value", serializedValue))

					metrics.GetSentMessagesCounter().Add(ctx, 1, metric.WithAttributeSet(
						spanAttr,
					))

					return nil
				}
			case <-ctx.Done():
				// observability: record error
				span.RecordError(ctx.Err())
				span.SetStatus(codes.Error, "Context cancelled while waiting for delivery confirmation")
				p.logger.Error("Context cancelled while waiting for delivery confirmation", zap.Error(ctx.Err()), zap.String("topic", topic))
				return ctx.Err()
			}
		}

		if err != nil {
			metrics.GetUnsuccessfulMessagesCounter().Add(ctx, 1, metric.WithAttributeSet(spanAttr))
			if attempt >= p.retryStrategy.MaxRetries() { // Updated to use MaxRetries()
				// observability: add event
				span.AddEvent("Exceeded max retries", trace.WithAttributes(
					attribute.Int("max_retries", p.retryStrategy.MaxRetries()),
				))
				span.RecordError(err)
				span.SetStatus(codes.Error, "Failed to produce message after maximum retries")
				p.logger.Error("Failed to produce message after maximum retries", zap.Error(err), zap.String("topic", topic))
				return err
			}

			// Retry
			metrics.GetUnsuccessfulRetriesCounter().Add(ctx, 1, metric.WithAttributeSet(spanAttr))

			backoff := p.retryStrategy.NextInterval(attempt)
			p.logger.Error("Error producing message. Retrying...", zap.Error(err), zap.Int("attempt", attempt), zap.Duration("backoff", backoff), zap.String("topic", topic))

			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				// observability: record error
				span.RecordError(ctx.Err())
				span.SetStatus(codes.Error, "Context cancelled before retrying")
				p.logger.Error("Context cancelled before retrying", zap.Error(ctx.Err()), zap.String("topic", topic))
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				// Proceed to next retry attempt
			}
			timer.Stop()
		}
	}
}

// Close cleans up the producer.
func (p *Producer) Close() {
	p.producer.Close()
}

func extractContextValues(ctx context.Context) map[string]string {
	values := make(map[string]string)
	if ctxValues, ok := ctx.Value(common.MapKey).(map[string]string); ok {
		for k, v := range ctxValues {
			values[k] = v
		}
	}
	return values
}
