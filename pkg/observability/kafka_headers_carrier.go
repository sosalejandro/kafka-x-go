package observability

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.opentelemetry.io/otel/propagation"
)

type KafkaHeadersCarrier struct {
	headers []kafka.Header
}

func NewKafkaHeadersCarrier(headers []kafka.Header) *KafkaHeadersCarrier {
	return &KafkaHeadersCarrier{headers: headers}
}

func (c *KafkaHeadersCarrier) Get(key string) string {
	for _, h := range c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *KafkaHeadersCarrier) Set(key string, val string) {
	c.headers = append(c.headers, kafka.Header{Key: key, Value: []byte(val)})
}

func (c *KafkaHeadersCarrier) Keys() []string {
	keys := make([]string, len(c.headers))
	for i, h := range c.headers {
		keys[i] = h.Key
	}
	return keys
}

func (c *KafkaHeadersCarrier) GetHeaders() []kafka.Header {
	return c.headers
}

// Ensure KafkaHeadersCarrier implements propagation.TextMapCarrier
var _ propagation.TextMapCarrier = (*KafkaHeadersCarrier)(nil)
