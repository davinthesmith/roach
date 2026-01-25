package service

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	libkafka "github.com/roach/weatherlink-lib/kafka"
)

// scanExistingKeys scans Kafka topics to find existing message keys
// Returns error if scanning fails
//
// This builds an in-memory cache of all existing keys (lsid:timestamp) to prevent
// duplicate messages from being published during backfill operations.
//
// Progress is logged every 10,000 messages to show scanning activity.
func (s *Service) scanExistingKeys(ctx context.Context, topics []string) error {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:29092"
	}

	// Create temporary consumer for scanning
	groupID := fmt.Sprintf("backfill-scanner-%d", time.Now().Unix())
	consumer, err := libkafka.NewScanner(broker, groupID)
	if err != nil {
		return fmt.Errorf("failed to create scanner: %w", err)
	}
	defer consumer.Close()

	// Subscribe to all weather topics
	if err := consumer.SubscribeTopics(topics, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	// Get partition metadata to find end offsets
	metadata, err := consumer.GetMetadata(nil, false, 30000)
	if err != nil {
		return fmt.Errorf("failed to get metadata: %w", err)
	}

	// Calculate total messages across all topics
	totalMessages := 0
	endOffsets := make(map[string]map[int32]int64)
	for _, topic := range topics {
		topicMetadata, exists := metadata.Topics[topic]
		if !exists {
			log.Printf("  Topic %s does not exist yet, skipping", topic)
			continue
		}

		endOffsets[topic] = make(map[int32]int64)
		for _, partition := range topicMetadata.Partitions {
			// Get high water mark (end offset)
			low, high, err := consumer.QueryWatermarkOffsets(topic, partition.ID, 5000)
			if err != nil {
				log.Printf("  Warning: Failed to query offsets for %s[%d]: %v", topic, partition.ID, err)
				continue
			}
			endOffsets[topic][partition.ID] = high
			totalMessages += int(high - low)
			log.Printf("  Topic %s[%d]: %d messages (offset %d to %d)", topic, partition.ID, high-low, low, high)
		}
	}

	if totalMessages == 0 {
		log.Println("  No existing messages found in topics")
		return nil
	}

	log.Printf("  Scanning %d total messages across topics...", totalMessages)

	// Scan messages and extract keys
	messagesScanned := 0
	lastLogAt := 0

scanLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Poll for messages
			msg, err := consumer.ReadMessage(1000 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					// Check if we've reached end of all partitions
					reachedEnd := true
					for topic, partitions := range endOffsets {
						for partitionID, endOffset := range partitions {
							// Get current position
							position, err := consumer.Position([]kafka.TopicPartition{
								{Topic: &topic, Partition: partitionID},
							})
							if err != nil {
								continue
							}
							if len(position) > 0 && position[0].Offset < kafka.Offset(endOffset) {
								reachedEnd = false
								break
							}
						}
						if !reachedEnd {
							break
						}
					}
					if reachedEnd {
						log.Printf("  Reached end of all topics")
						break scanLoop
					}
					continue
				}
				return fmt.Errorf("failed to read message: %w", err)
			}

			// Extract key from message
			key := string(msg.Key)
			if key != "" {
				s.keysMutex.Lock()
				s.existingKeys[key] = true
				s.keysMutex.Unlock()
			}

			messagesScanned++

			// Log progress every 10,000 messages
			if messagesScanned-lastLogAt >= 10000 {
				log.Printf("  Scanned %d/%d messages...", messagesScanned, totalMessages)
				lastLogAt = messagesScanned
			}

			// Check if we've scanned all expected messages
			if messagesScanned >= totalMessages {
				break scanLoop
			}
		}
	}

	log.Printf("  Scan complete: Found %d unique keys from %d messages", len(s.existingKeys), messagesScanned)
	return nil
}
