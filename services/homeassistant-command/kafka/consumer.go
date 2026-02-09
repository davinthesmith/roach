package kafka

import (
	"github.com/segmentio/kafka-go"

	"homeassistant-command/models"
)

// NewCommandReader creates a Kafka reader for the command topic.
func NewCommandReader(cfg models.Config) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.KafkaBroker},
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaConsumerGroup,
	})
}
