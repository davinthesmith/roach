package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer handles publishing messages to Kafka
type Producer struct {
	writer *kafka.Writer
}

// NewProducer creates a new Kafka producer
func NewProducer(broker string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(broker),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
			Async:                  false,
		},
	}
}

// Publish publishes data to a Kafka topic with headers
func (p *Producer) Publish(ctx context.Context, topic string, data interface{}, headers map[string]string) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// Convert headers to kafka.Header format
	kafkaHeaders := make([]kafka.Header, 0, len(headers))
	for key, value := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   key,
			Value: []byte(value),
		})
	}

	msg := kafka.Message{
		Topic:   topic,
		Value:   jsonData,
		Headers: kafkaHeaders,
		Time:    time.Now(),
	}

	return p.writer.WriteMessages(ctx, msg)
}

// Close closes the Kafka producer
func (p *Producer) Close() error {
	return p.writer.Close()
}
