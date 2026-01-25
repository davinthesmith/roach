package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	KafkaBroker string
	PostgresDSN string
	LogLevel    string
	BatchSize   int
}

type Device struct {
	ID           int
	LSID         int
	SensorType   int
	Category     string
	Manufacturer string
	ProductName  string
}

type Tag struct {
	ID       int
	DeviceID int
	TagName  string
	DataType string
}

type Materializer struct {
	config          Config
	db              *sql.DB
	metadataReader  *kafka.Reader
	deviceCache     map[int]*Device
	tagCache        map[string]*Tag
	cacheMutex      sync.RWMutex
}

func loadConfig() Config {
	batchSize := 100
	if bs := os.Getenv("BATCH_SIZE"); bs != "" {
		if val, err := strconv.Atoi(bs); err == nil {
			batchSize = val
		}
	}

	return Config{
		KafkaBroker: getEnvOrDefault("KAFKA_BROKER", "kafka:29092"),
		PostgresDSN: os.Getenv("POSTGRES_DSN"),
		LogLevel:    getEnvOrDefault("LOG_LEVEL", "info"),
		BatchSize:   batchSize,
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func NewMaterializer(config Config) (*Materializer, error) {
	db, err := sql.Open("postgres", config.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	log.Println("Connected to PostgreSQL")

	// Create separate reader for metadata topics
	metadataReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{config.KafkaBroker},
		GroupID:     "weather-sql-metadata",
		GroupTopics: []string{"weather.metadata.sensors"},
		MinBytes:    10e3,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})

	m := &Materializer{
		config:         config,
		db:             db,
		metadataReader: metadataReader,
		deviceCache:    make(map[int]*Device),
		tagCache:       make(map[string]*Tag),
	}

	return m, nil
}

func (m *Materializer) loadDevices(ctx context.Context) error {
	log.Println("Loading devices from database...")

	rows, err := m.db.QueryContext(ctx, `
		SELECT id, lsid, sensor_type, category, manufacturer, product_name
		FROM devices
	`)
	if err != nil {
		return fmt.Errorf("failed to query devices: %w", err)
	}
	defer rows.Close()

	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()

	count := 0
	for rows.Next() {
		var d Device
		if err := rows.Scan(&d.ID, &d.LSID, &d.SensorType, &d.Category, &d.Manufacturer, &d.ProductName); err != nil {
			return fmt.Errorf("failed to scan device: %w", err)
		}
		m.deviceCache[d.LSID] = &d
		count++
	}

	log.Printf("Loaded %d devices into cache", count)
	return nil
}

func (m *Materializer) loadTags(ctx context.Context) error {
	log.Println("Loading tags from database...")

	rows, err := m.db.QueryContext(ctx, `
		SELECT id, device_id, tag_name, data_type
		FROM tags
	`)
	if err != nil {
		return fmt.Errorf("failed to query tags: %w", err)
	}
	defer rows.Close()

	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()

	count := 0
	for rows.Next() {
		var t Tag
		if err := rows.Scan(&t.ID, &t.DeviceID, &t.TagName, &t.DataType); err != nil {
			return fmt.Errorf("failed to scan tag: %w", err)
		}
		key := fmt.Sprintf("%d:%s", t.DeviceID, t.TagName)
		m.tagCache[key] = &t
		count++
	}

	log.Printf("Loaded %d tags into cache", count)
	return nil
}

func (m *Materializer) processMetadataMessage(ctx context.Context, msg kafka.Message) error {
	var metadata map[string]interface{}
	if err := json.Unmarshal(msg.Value, &metadata); err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	lsid, ok := metadata["lsid"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid lsid in metadata")
	}

	// Upsert device
	_, err := m.db.ExecContext(ctx, `
		INSERT INTO devices (lsid, sensor_type, category, manufacturer, product_name, 
			station_id, station_id_uuid, station_name, latitude, longitude, elevation, metadata, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
		ON CONFLICT (lsid) DO UPDATE SET
			sensor_type = EXCLUDED.sensor_type,
			category = EXCLUDED.category,
			manufacturer = EXCLUDED.manufacturer,
			product_name = EXCLUDED.product_name,
			station_id = EXCLUDED.station_id,
			station_id_uuid = EXCLUDED.station_id_uuid,
			station_name = EXCLUDED.station_name,
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			elevation = EXCLUDED.elevation,
			metadata = EXCLUDED.metadata,
			updated_at = NOW()
	`,
		int(lsid),
		getFloat(metadata, "sensor_type"),
		getString(metadata, "category"),
		getString(metadata, "manufacturer"),
		getString(metadata, "product_name"),
		getFloat(metadata, "station_id"),
		getString(metadata, "station_id_uuid"),
		getString(metadata, "station_name"),
		getFloat(metadata, "latitude"),
		getFloat(metadata, "longitude"),
		getFloat(metadata, "elevation"),
		msg.Value,
	)

	if err != nil {
		return fmt.Errorf("failed to upsert device: %w", err)
	}

	log.Printf("Upserted device LSID=%d", int(lsid))

	// Reload devices to refresh cache
	return m.loadDevices(ctx)
}

func (m *Materializer) processDataMessage(ctx context.Context, msg kafka.Message) error {
	// Extract LSID from headers
	var lsid int
	var timestamp int64
	
	for _, h := range msg.Headers {
		if h.Key == "lsid" {
			val, _ := strconv.Atoi(string(h.Value))
			lsid = val
		}
		if h.Key == "timestamp" {
			val, _ := strconv.ParseInt(string(h.Value), 10, 64)
			timestamp = val
		}
	}

	if lsid == 0 {
		return fmt.Errorf("missing lsid in message headers")
	}

	// Lookup device in cache
	m.cacheMutex.RLock()
	device, exists := m.deviceCache[lsid]
	m.cacheMutex.RUnlock()

	if !exists {
		return m.saveOrphanedMessage(ctx, msg, lsid, timestamp, "", "missing_device")
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
		tagKey := fmt.Sprintf("%d:%s", device.ID, fieldName)
		m.cacheMutex.RLock()
		tag, tagExists := m.tagCache[tagKey]
		m.cacheMutex.RUnlock()

		if !tagExists {
			// Create tag on the fly
			dataType := determineDataType(fieldValue)
			tagID, err := m.createTag(ctx, device.ID, fieldName, dataType)
			if err != nil {
				log.Printf("Failed to create tag %s: %v", fieldName, err)
				m.saveOrphanedMessage(ctx, msg, lsid, timestamp, fieldName, "failed_to_create_tag")
				continue
			}

			tag = &Tag{
				ID:       tagID,
				DeviceID: device.ID,
				TagName:  fieldName,
				DataType: dataType,
			}

			m.cacheMutex.Lock()
			m.tagCache[tagKey] = tag
			m.cacheMutex.Unlock()

			log.Printf("Created tag %s (type: %s) for device %d", fieldName, dataType, device.ID)
		}

		// Insert record into appropriate table
		if err := m.insertRecord(ctx, tag, device.ID, fieldValue, timestamp); err != nil {
			log.Printf("Failed to insert record for tag %s: %v", fieldName, err)
		}
	}

	return nil
}

func (m *Materializer) createTag(ctx context.Context, deviceID int, tagName, dataType string) (int, error) {
	var tagID int
	err := m.db.QueryRowContext(ctx, `
		INSERT INTO tags (device_id, tag_name, data_type)
		VALUES ($1, $2, $3)
		ON CONFLICT (device_id, tag_name) DO UPDATE SET
			data_type = EXCLUDED.data_type,
			updated_at = NOW()
		RETURNING id
	`, deviceID, tagName, dataType).Scan(&tagID)

	return tagID, err
}

func (m *Materializer) insertRecord(ctx context.Context, tag *Tag, deviceID int, value interface{}, timestamp int64) error {
	if value == nil {
		_, err := m.db.ExecContext(ctx, `
			INSERT INTO records_null (tag_id, device_id, timestamp)
			VALUES ($1, $2, $3)
			ON CONFLICT (tag_id, timestamp) DO NOTHING
		`, tag.ID, deviceID, timestamp)
		return err
	}

	switch tag.DataType {
	case "numeric":
		var numValue float64
		switch v := value.(type) {
		case float64:
			numValue = v
		case int:
			numValue = float64(v)
		case int64:
			numValue = float64(v)
		default:
			return fmt.Errorf("cannot convert %T to numeric", value)
		}

		_, err := m.db.ExecContext(ctx, `
			INSERT INTO records_numeric (tag_id, device_id, value, timestamp)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (tag_id, timestamp) DO NOTHING
		`, tag.ID, deviceID, numValue, timestamp)
		return err

	case "text":
		strValue := fmt.Sprintf("%v", value)
		_, err := m.db.ExecContext(ctx, `
			INSERT INTO records_text (tag_id, device_id, value, timestamp)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (tag_id, timestamp) DO NOTHING
		`, tag.ID, deviceID, strValue, timestamp)
		return err

	default:
		return fmt.Errorf("unknown data type: %s", tag.DataType)
	}
}

func (m *Materializer) saveOrphanedMessage(ctx context.Context, msg kafka.Message, lsid int, timestamp int64, tagName, reason string) error {
	headers, _ := json.Marshal(headersToMap(msg.Headers))

	_, err := m.db.ExecContext(ctx, `
		INSERT INTO orphaned_messages (topic, partition, offset, lsid, timestamp, tag_name, reason, message_headers, message_body)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (topic, partition, offset) DO NOTHING
	`, msg.Topic, msg.Partition, msg.Offset, lsid, timestamp, tagName, reason, headers, msg.Value)

	return err
}

func (m *Materializer) listenForMetadata(ctx context.Context) {
	log.Println("Starting metadata listener...")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := m.metadataReader.FetchMessage(ctx)
			if err != nil {
				log.Printf("Error fetching metadata message: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if err := m.processMetadataMessage(ctx, msg); err != nil {
				log.Printf("Error processing metadata message: %v", err)
			}

			if err := m.metadataReader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Error committing metadata message: %v", err)
			}
		}
	}
}

func (m *Materializer) Start(ctx context.Context) error {
	log.Println("Starting ROACH Weather SQL Materializer...")

	// Load initial data
	if err := m.loadDevices(ctx); err != nil {
		log.Printf("Warning: Failed to load devices: %v", err)
	}

	if err := m.loadTags(ctx); err != nil {
		log.Printf("Warning: Failed to load tags: %v", err)
	}

	// Start metadata listener in background
	go m.listenForMetadata(ctx)

	// Subscribe to data topics using pattern
	log.Println("Subscribing to weather data topics...")
	
	// List all topics and filter for weather.* (excluding metadata)
	conn, err := kafka.Dial("tcp", m.config.KafkaBroker)
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

	// Create readers for each topic (simplified approach)
	// In production, you'd want a more sophisticated multi-topic consumer
	readers := make([]*kafka.Reader, 0)
	for topic := range topics {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{m.config.KafkaBroker},
			GroupID:     "weather-sql-materializer",
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
				ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				msg, err := reader.FetchMessage(ctx)
				cancel()

				if err != nil {
					continue
				}

				if err := m.processDataMessage(context.Background(), msg); err != nil {
					log.Printf("Error processing message from %s: %v", msg.Topic, err)
				}

				if err := reader.CommitMessages(context.Background(), msg); err != nil {
					log.Printf("Error committing message: %v", err)
				}
			}
		}
	}
}

func (m *Materializer) Close() error {
	if m.metadataReader != nil {
		m.metadataReader.Close()
	}
	if m.db != nil {
		m.db.Close()
	}
	return nil
}

// Helper functions

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

func headersToMap(headers []kafka.Header) map[string]string {
	m := make(map[string]string)
	for _, h := range headers {
		m[h.Key] = string(h.Value)
	}
	return m
}

func getString(m map[string]interface{}, key string) string {
	if val, ok := m[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func getFloat(m map[string]interface{}, key string) interface{} {
	if val, ok := m[key]; ok {
		return val
	}
	return nil
}

func main() {
	log.Println("Starting ROACH Weather SQL Materializer...")

	config := loadConfig()

	if config.PostgresDSN == "" {
		log.Fatal("POSTGRES_DSN is required")
	}

	log.Printf("Configuration loaded:")
	log.Printf("  - Kafka Broker: %s", config.KafkaBroker)
	log.Printf("  - Batch Size: %d", config.BatchSize)

	materializer, err := NewMaterializer(config)
	if err != nil {
		log.Fatalf("Failed to create materializer: %v", err)
	}
	defer materializer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutdown signal received, stopping...")
		cancel()
	}()

	if err := materializer.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Materializer error: %v", err)
	}

	log.Println("Materializer stopped")
}
