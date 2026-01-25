package kafka

import (
	"github.com/segmentio/kafka-go"

	"weather-sql/models"
)

// NewMetadataReader creates a Kafka reader for metadata topic
func NewMetadataReader(config models.Config) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{config.KafkaBroker},
		Topic:   "weather.metadata.sensors",
		GroupID: "weather-sql-metadata",
	})
}

// NewCatalogReader creates a Kafka reader for catalog topic
func NewCatalogReader(config models.Config) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{config.KafkaBroker},
		Topic:   "weather.metadata.catalog",
		GroupID: "weather-sql-catalog",
	})
}

// NewDataReader creates a Kafka reader for data topics using pattern matching
func NewDataReader(config models.Config) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{config.KafkaBroker},
		GroupID: "weather-sql-data",
		Topic:   "weather.*", // Pattern to match all weather data topics
	})
}
