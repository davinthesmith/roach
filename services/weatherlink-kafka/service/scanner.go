package service

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	libkafka "weatherlink-kafka/kafka"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// scanExistingKeys scans Kafka topics to find existing message keys.
func (s *Service) scanExistingKeys(ctx context.Context, topics []string, cache map[string]struct{}, mutex *sync.RWMutex) error {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:29092"
	}

	groupID := fmt.Sprintf("weatherlink-kafka-scanner-%d", time.Now().Unix())
	consumer, err := libkafka.NewScanner(broker, groupID)
	if err != nil {
		return fmt.Errorf("failed to create scanner: %w", err)
	}
	defer consumer.Close()

	if err := consumer.SubscribeTopics(topics, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	metadata, err := consumer.GetMetadata(nil, false, 30000)
	if err != nil {
		return fmt.Errorf("failed to get metadata: %w", err)
	}

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

	messagesScanned := 0
	lastLogAt := 0

scanLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := consumer.ReadMessage(1000 * time.Millisecond)
			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					reachedEnd := true
					for topic, partitions := range endOffsets {
						for partitionID, endOffset := range partitions {
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

			key := string(msg.Key)
			if key != "" && cache != nil {
				mutex.Lock()
				cache[key] = struct{}{}
				mutex.Unlock()
			}

			messagesScanned++
			if messagesScanned-lastLogAt >= 10000 {
				log.Printf("  Scanned %d/%d messages...", messagesScanned, totalMessages)
				lastLogAt = messagesScanned
			}

			if messagesScanned >= totalMessages {
				break scanLoop
			}
		}
	}

	uniqueKeys := 0
	if cache != nil {
		uniqueKeys = len(cache)
	}
	log.Printf("  Scan complete: Found %d unique keys from %d messages", uniqueKeys, messagesScanned)
	return nil
}
