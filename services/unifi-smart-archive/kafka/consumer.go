package kafka

import (
	"github.com/segmentio/kafka-go"

	"unifi-smart-archive/models"
)

// NewReader creates a Kafka reader for the smart events topic.
func NewReader(cfg models.Config) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.KafkaBroker},
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaConsumerGroup,
	})
}
