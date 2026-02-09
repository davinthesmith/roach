package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Producer handles publishing messages to Kafka with idempotent guarantees.
type Producer struct {
	producer *kafka.Producer
}

// NewProducer creates a new Kafka producer with idempotent settings.
func NewProducer(broker string) (*Producer, error) {
	config := &kafka.ConfigMap{
		// Connection
		"bootstrap.servers": broker,

		// Idempotency (exactly-once semantics)
		"enable.idempotence":                    true,
		"acks":                                  "all",
		"max.in.flight.requests.per.connection": 5,
		"retries":                               2147483647,

		// Compression and batching — LZ4 less effective on binary images
		// but still reduces header/metadata overhead
		"compression.type": "lz4",
		"linger.ms":        50,
		"batch.size":       1048576, // 1MB batches (frames are larger than JSON)

		// Timeouts
		"request.timeout.ms":  30000,
		"delivery.timeout.ms": 120000,

		// Message size — frames can be large
		"message.max.bytes": 10485760, // 10MB max message

		// Auto topic creation
		"allow.auto.create.topics": true,
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	// Start delivery report handler in background
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				}
			}
		}
	}()

	return &Producer{producer: producer}, nil
}

// PublishFrame publishes a raw frame to a Kafka topic with key and headers.
func (p *Producer) PublishFrame(ctx context.Context, topic string, key string, data []byte, headers map[string]string) error {
	kafkaHeaders := make([]kafka.Header, 0, len(headers))
	for k, value := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   k,
			Value: []byte(value),
		})
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:       []byte(key),
		Value:     data,
		Headers:   kafkaHeaders,
		Timestamp: time.Now(),
	}

	deliveryChan := make(chan kafka.Event, 1)
	err := p.producer.Produce(msg, deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	select {
	case e := <-deliveryChan:
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(30 * time.Second):
		return fmt.Errorf("delivery timeout after 30 seconds")
	}
}

// Flush waits for all pending messages to be delivered.
func (p *Producer) Flush(timeoutMs int) int {
	return p.producer.Flush(timeoutMs)
}

// Close flushes pending messages and closes the producer.
func (p *Producer) Close() error {
	remaining := p.producer.Flush(10000)
	if remaining > 0 {
		return fmt.Errorf("failed to flush %d messages", remaining)
	}
	p.producer.Close()
	return nil
}
