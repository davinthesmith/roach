package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// NewScanner creates a one-time consumer for scanning topics from the beginning
// Used for deduplication: reads all existing messages to build a cache of keys
//
// Configuration:
// - group.id: Temporary group ID for scanning (not persistent)
// - auto.offset.reset: earliest - Start from beginning of topic
// - enable.auto.commit: false - Don't commit offsets (one-time scan)
func NewScanner(broker string, groupID string) (*kafka.Consumer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers":  broker,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest", // Read from beginning
		"enable.auto.commit": false,      // Don't commit offsets (one-time scan)
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return consumer, nil
}
