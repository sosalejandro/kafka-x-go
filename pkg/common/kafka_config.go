package common

// KafkaConfig holds configuration for Kafka clients.
type KafkaConfig struct {
	BootstrapServers  string
	SchemaRegistryURL string
	GroupID           string
}
