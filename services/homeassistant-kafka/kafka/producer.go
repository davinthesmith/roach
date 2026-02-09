package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Producer handles publishing messages to Kafka with idempotent guarantees
type Producer struct {
	producer *kafka.Producer
}

// NewProducer creates a new Kafka producer with idempotent settings
//
// IDEMPOTENCY: Uses confluent-kafka-go which supports Kafka's idempotent producer
// feature (enable.idempotence=true). This provides exactly-once semantics by:
// 1. Assigning each producer a unique Producer ID (PID)
// 2. Adding sequence numbers to each message
// 3. Broker deduplicates based on (PID, sequence number)
//
// This eliminates duplicate messages on network failures with retries:
// - Producer sends message successfully
// - Kafka commits the message
// - Network fails before acknowledgment reaches producer
// - Producer retries → Broker detects duplicate via sequence number and drops it
//
// Configuration:
// - enable.idempotence=true: Enables idempotent producer
// - acks=all: Wait for all in-sync replicas (required for idempotence)
// - max.in.flight.requests.per.connection=5: Max unacknowledged requests
// - retries=2147483647: Unlimited retries (safe with idempotence)
// - compression.type=lz4: 60-70% storage savings
// - linger.ms=50: Small delay for batching (better compression)
// - batch.size=100000: 100KB batches
func NewProducer(broker string) (*Producer, error) {
	config := &kafka.ConfigMap{
		// Connection
		"bootstrap.servers": broker,

		// Idempotency (exactly-once semantics)
		"enable.idempotence":                    true,
		"acks":                                  "all",
		"max.in.flight.requests.per.connection": 5,
		"retries":                               2147483647, // Unlimited retries (safe with idempotence)

		// Compression and batching
		"compression.type": "lz4",
		"linger.ms":        50,     // Wait up to 50ms to batch messages
		"batch.size":       100000, // 100KB batches

		// Timeouts
		"request.timeout.ms":  30000,  // 30 seconds
		"delivery.timeout.ms": 120000, // 2 minutes total

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
					// Delivery failed after all retries
					// In production, this should log to monitoring system
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				}
			}
		}
	}()

	return &Producer{producer: producer}, nil
}

// Publish publishes data to a Kafka topic with key and headers
// Returns error only if serialization fails. Delivery errors are handled asynchronously.
func (p *Producer) Publish(ctx context.Context, topic string, key string, data interface{}, headers map[string]string) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	// Convert headers to kafka.Header format
	kafkaHeaders := make([]kafka.Header, 0, len(headers))
	for k, value := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   k,
			Value: []byte(value),
		})
	}

	// Create message
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:       []byte(key),
		Value:     jsonData,
		Headers:   kafkaHeaders,
		Timestamp: time.Now(),
	}

	// Produce message (async with delivery channel)
	deliveryChan := make(chan kafka.Event, 1)
	err = p.producer.Produce(msg, deliveryChan)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	// Wait for delivery report (synchronous for error handling)
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

// PublishAsync publishes data to a Kafka topic asynchronously without waiting for delivery
// This is much faster for bulk operations. Errors are logged via the event handler.
func (p *Producer) PublishAsync(ctx context.Context, topic string, key string, data interface{}, headers map[string]string) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	// Convert headers to kafka.Header format
	kafkaHeaders := make([]kafka.Header, 0, len(headers))
	for k, value := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   k,
			Value: []byte(value),
		})
	}

	// Create message
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:       []byte(key),
		Value:     jsonData,
		Headers:   kafkaHeaders,
		Timestamp: time.Now(),
	}

	// Produce message (async - don't wait for delivery)
	// Delivery reports are handled by the background goroutine
	err = p.producer.Produce(msg, nil)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	return nil
}

// Flush waits for all pending messages to be delivered
func (p *Producer) Flush(timeoutMs int) int {
	return p.producer.Flush(timeoutMs)
}

// Close flushes pending messages and closes the producer
func (p *Producer) Close() error {
	// Flush any outstanding messages (wait up to 10 seconds)
	remaining := p.producer.Flush(10000)
	if remaining > 0 {
		return fmt.Errorf("failed to flush %d messages", remaining)
	}
	p.producer.Close()
	return nil
}
