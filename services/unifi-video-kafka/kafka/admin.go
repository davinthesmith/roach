package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	// TopicRetentionMs is the retention period for video topics (30 minutes).
	TopicRetentionMs = "1800000"
	// TopicSegmentMs forces segment rolling every 1 minute so retention can
	// actually delete old data. Without this, the default segment.bytes (1GB)
	// means the active segment never rolls and nothing gets cleaned up.
	TopicSegmentMs = "60000"
)

// EnsureTopic creates a Kafka topic with 30-minute retention if it does not
// already exist. If the topic exists, its config is updated to match.
func EnsureTopic(broker, topic string) error {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": broker,
	})
	if err != nil {
		return fmt.Errorf("create admin client: %w", err)
	}
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	topicConfig := map[string]string{
		"retention.ms": TopicRetentionMs,
		"segment.ms":   TopicSegmentMs,
	}

	results, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
			Config:            topicConfig,
		},
	})
	if err != nil {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}

	for _, result := range results {
		if result.Error.Code() == kafka.ErrTopicAlreadyExists {
			// Topic exists — update its config to ensure retention and segment
			// settings are correct (handles upgrades from older config).
			if err := updateTopicConfig(admin, topic, topicConfig); err != nil {
				log.Printf("[%s] WARNING: Failed to update topic config: %v", topic, err)
			}
			return nil
		}
		if result.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("create topic %s: %s", topic, result.Error.String())
		}
	}

	log.Printf("Created topic %s (retention=%sms, segment=%sms)", topic, TopicRetentionMs, TopicSegmentMs)
	return nil
}

// updateTopicConfig alters an existing topic's config entries.
func updateTopicConfig(admin *kafka.AdminClient, topic string, config map[string]string) error {
	var entries []kafka.ConfigEntry
	for k, v := range config {
		entries = append(entries, kafka.ConfigEntry{
			Name:  k,
			Value: v,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	results, err := admin.AlterConfigs(ctx, []kafka.ConfigResource{
		{
			Type:   kafka.ResourceTopic,
			Name:   topic,
			Config: entries,
		},
	})
	if err != nil {
		return fmt.Errorf("alter config: %w", err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("alter config for %s: %s", topic, result.Error.String())
		}
	}

	log.Printf("Updated topic %s config (retention=%sms, segment=%sms)", topic, TopicRetentionMs, TopicSegmentMs)
	return nil
}
