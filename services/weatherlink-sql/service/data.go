package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"weatherlink-sql/cache"
	"weatherlink-sql/models"
	"weatherlink-sql/repository"
)

// DataProcessor handles data message processing
type DataProcessor struct {
	tagRepo    *repository.TagRepository
	recordRepo *repository.RecordRepository
	orphanRepo *repository.OrphanRepository
	cache      *cache.Cache
}

// NewDataProcessor creates a new DataProcessor
func NewDataProcessor(tagRepo *repository.TagRepository, recordRepo *repository.RecordRepository, orphanRepo *repository.OrphanRepository, cache *cache.Cache) *DataProcessor {
	return &DataProcessor{
		tagRepo:    tagRepo,
		recordRepo: recordRepo,
		orphanRepo: orphanRepo,
		cache:      cache,
	}
}

// LoadTags loads tags from database into cache
func (p *DataProcessor) LoadTags(ctx context.Context) error {
	tags, err := p.tagRepo.LoadAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to load tags: %w", err)
	}

	for _, tag := range tags {
		p.cache.SetTag(tag.DeviceID, tag.TagName, tag)
	}

	log.Printf("Loaded %d tags into cache", len(tags))
	return nil
}

// ProcessMessage processes a data message
func (p *DataProcessor) ProcessMessage(ctx context.Context, msg kafka.Message) error {
	// Extract LSID and other metadata from headers
	var lsid int
	var timestamp int64
	var sensorType int
	var dataStructureType int

	for _, h := range msg.Headers {
		if h.Key == "lsid" {
			val, _ := strconv.Atoi(string(h.Value))
			lsid = val
		}
		if h.Key == "timestamp" {
			val, _ := strconv.ParseInt(string(h.Value), 10, 64)
			timestamp = val
		}
		if h.Key == "sensor_type" {
			val, _ := strconv.Atoi(string(h.Value))
			sensorType = val
		}
		if h.Key == "data_structure_type" {
			val, _ := strconv.Atoi(string(h.Value))
			dataStructureType = val
		}
	}

	if lsid == 0 {
		return fmt.Errorf("missing lsid in message headers")
	}

	// Lookup device in cache
	device := p.cache.GetDevice(lsid)
	if device == nil {
		return p.orphanRepo.Save(ctx, msg, lsid, timestamp, "", "missing_device")
	}

	// Update device's data_structure_type if we got it from headers
	if dataStructureType > 0 && (device.DataStructureType == nil || *device.DataStructureType != dataStructureType) {
		device.DataStructureType = &dataStructureType
		// Note: We could update DB here, but for simplicity we rely on cache invalidation
	}

	// Parse message body
	var data map[string]interface{}
	if err := json.Unmarshal(msg.Value, &data); err != nil {
		return fmt.Errorf("failed to parse message body: %w", err)
	}

	// Process each field
	for fieldName, fieldValue := range data {
		// Skip timestamp field as it's in headers
		if fieldName == "ts" {
			continue
		}

		// Lookup tag in cache
		tag := p.cache.GetTag(device.ID, fieldName)

		if tag == nil {
			// Create tag on the fly with enrichment from catalog
			dataType := determineDataType(fieldValue)

			// Lookup field metadata from catalog
			var unit *string
			var description *string
			var metadata map[string]interface{}
			if sensorType > 0 && dataStructureType > 0 {
				if fieldMeta := p.cache.GetCatalogMetadata(sensorType, dataStructureType, fieldName); fieldMeta != nil {
					unit = &fieldMeta.Units
					description = &fieldMeta.Description
					// Include complete metadata from catalog
					metadata = map[string]interface{}{
						"field_type":          fieldMeta.FieldType,
						"data_structure_type": fieldMeta.DataStructureType,
						"sensor_type":         fieldMeta.SensorType,
					}
					// Merge in raw metadata if available
					for k, v := range fieldMeta.RawMetadata {
						metadata[k] = v
					}
				}
			}

			tagID, err := p.tagRepo.Create(ctx, device.ID, fieldName, dataType, unit, description, metadata)
			if err != nil {
				log.Printf("Failed to create tag %s: %v", fieldName, err)
				p.orphanRepo.Save(ctx, msg, lsid, timestamp, fieldName, "failed_to_create_tag")
				continue
			}

			tag = &models.Tag{
				ID:          tagID,
				DeviceID:    device.ID,
				TagName:     fieldName,
				DataType:    dataType,
				Unit:        unit,
				Description: description,
			}

			p.cache.SetTag(device.ID, fieldName, tag)

			log.Printf("Created tag %s (type: %s, unit: %v) for device %d", fieldName, dataType, unit, device.ID)
		}

		// Insert record into appropriate table
		if err := p.recordRepo.Insert(ctx, tag, fieldValue, timestamp); err != nil {
			log.Printf("Failed to insert record for tag %s: %v", fieldName, err)
		}
	}

	return nil
}

// Listen listens for data messages on pattern-matched topics
func (p *DataProcessor) Listen(ctx context.Context, kafkaBroker string) error {
	log.Println("Subscribing to weather data topics...")

	// List all topics and filter for weather.* (excluding metadata)
	conn, err := kafka.Dial("tcp", kafkaBroker)
	if err != nil {
		return fmt.Errorf("failed to dial kafka: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to read partitions: %w", err)
	}

	topics := make(map[string]bool)
	for _, p := range partitions {
		if len(p.Topic) > 8 && p.Topic[:8] == "weather." {
			// Exclude metadata topics
			if len(p.Topic) >= 17 && p.Topic[:17] == "weather.metadata." {
				continue
			}
			topics[p.Topic] = true
		}
	}

	log.Printf("Found %d data topics to consume", len(topics))

	// Create readers for each topic
	readers := make([]*kafka.Reader, 0)
	for topic := range topics {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{kafkaBroker},
			GroupID:     "weatherlink-sql-data",
			GroupTopics: []string{topic},
			MinBytes:    10e3,
			MaxBytes:    10e6,
			StartOffset: kafka.LastOffset,
		})
		readers = append(readers, reader)
		log.Printf("Subscribed to topic: %s", topic)
	}

	// Process messages from all readers
	for {
		select {
		case <-ctx.Done():
			for _, r := range readers {
				r.Close()
			}
			return ctx.Err()
		default:
			// Read from each reader
			for _, reader := range readers {
				msgCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				msg, err := reader.FetchMessage(msgCtx)
				cancel()

				if err != nil {
					continue
				}

				if err := p.ProcessMessage(context.Background(), msg); err != nil {
					log.Printf("Error processing message from %s: %v", msg.Topic, err)
				}

				if err := reader.CommitMessages(context.Background(), msg); err != nil {
					log.Printf("Error committing message: %v", err)
				}
			}
		}
	}
}

// ListenWithWorkerPool listens for data messages and distributes them to worker pool
func (p *DataProcessor) ListenWithWorkerPool(ctx context.Context, kafkaBroker string, workerPool *WorkerPool) error {
	log.Println("Subscribing to weather data topics with worker pool...")

	// List all topics and filter for weather.* (excluding metadata)
	conn, err := kafka.Dial("tcp", kafkaBroker)
	if err != nil {
		return fmt.Errorf("failed to dial kafka: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to read partitions: %w", err)
	}

	topics := make(map[string]bool)
	for _, p := range partitions {
		if len(p.Topic) > 8 && p.Topic[:8] == "weather." {
			// Exclude metadata topics
			if len(p.Topic) >= 17 && p.Topic[:17] == "weather.metadata." {
				continue
			}
			topics[p.Topic] = true
		}
	}

	log.Printf("Found %d data topics to consume", len(topics))

	// Create readers for each topic
	readers := make([]*kafka.Reader, 0)
	for topic := range topics {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{kafkaBroker},
			GroupID:     "weatherlink-sql-data",
			GroupTopics: []string{topic},
			MinBytes:    10e3,
			MaxBytes:    10e6,
			StartOffset: kafka.LastOffset,
		})
		readers = append(readers, reader)
		log.Printf("Subscribed to topic: %s", topic)
	}

	// Distribute messages to worker pool
	for {
		select {
		case <-ctx.Done():
			for _, r := range readers {
				r.Close()
			}
			return ctx.Err()
		default:
			// Read from each reader
			for _, reader := range readers {
				msgCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				msg, err := reader.FetchMessage(msgCtx)
				cancel()

				if err != nil {
					continue
				}

				// Submit to worker pool
				if err := workerPool.SubmitMessage(msg); err != nil {
					log.Printf("Error submitting message to worker pool: %v", err)
					continue
				}

				// Commit immediately after submission
				if err := reader.CommitMessages(context.Background(), msg); err != nil {
					log.Printf("Error committing message: %v", err)
				}
			}
		}
	}
}

// Helper function
func determineDataType(value interface{}) string {
	if value == nil {
		return "null"
	}

	switch value.(type) {
	case float64, int, int64:
		return "numeric"
	case string:
		return "text"
	case bool:
		return "numeric"
	default:
		return "text"
	}
}
