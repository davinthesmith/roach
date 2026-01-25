package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/segmentio/kafka-go"

	"weatherlink-kafka-backfill/cache"
	"weatherlink-kafka-backfill/models"
	"weatherlink-kafka-backfill/repository"
)

// WorkerPool manages concurrent message processing
type WorkerPool struct {
	workers       int
	messageChan   chan kafka.Message
	batchWriter   *repository.BatchWriter
	tagRepo       *repository.TagRepository
	orphanRepo    *repository.OrphanRepository
	cache         *cache.Cache
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup

	// Metrics
	messagesProcessed int64
	processingErrors  int64
	mutex             sync.Mutex
}

// NewWorkerPool creates a new WorkerPool
func NewWorkerPool(
	workers int,
	batchWriter *repository.BatchWriter,
	tagRepo *repository.TagRepository,
	orphanRepo *repository.OrphanRepository,
	cache *cache.Cache,
) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		workers:     workers,
		messageChan: make(chan kafka.Message, workers*2), // Buffer 2x worker count
		batchWriter: batchWriter,
		tagRepo:     tagRepo,
		orphanRepo:  orphanRepo,
		cache:       cache,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start starts the worker pool
func (wp *WorkerPool) Start() {
	log.Printf("Starting worker pool with %d workers", wp.workers)

	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// worker processes messages from the channel
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case <-wp.ctx.Done():
			return
		case msg, ok := <-wp.messageChan:
			if !ok {
				return
			}

			if err := wp.processMessage(msg); err != nil {
				wp.incrementErrors()
			} else {
				wp.incrementProcessed()
			}
		}
	}
}

// SubmitMessage submits a message for processing
func (wp *WorkerPool) SubmitMessage(msg kafka.Message) error {
	select {
	case <-wp.ctx.Done():
		return fmt.Errorf("worker pool is shutting down")
	case wp.messageChan <- msg:
		return nil
	}
}

// processMessage processes a single message
func (wp *WorkerPool) processMessage(msg kafka.Message) error {
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
	device := wp.cache.GetDevice(lsid)
	if device == nil {
		err := wp.orphanRepo.Save(context.Background(), msg, lsid, timestamp, "", "missing_device")
		if err != nil {
			log.Printf("Failed to save orphaned message (missing_device) for lsid=%d: %v", lsid, err)
		}
		return fmt.Errorf("missing device for lsid=%d", lsid)
	}

	// Update device's data_structure_type if we got it from headers
	if dataStructureType > 0 && (device.DataStructureType == nil || *device.DataStructureType != dataStructureType) {
		device.DataStructureType = &dataStructureType
	}

	// Parse message body
	var data map[string]interface{}
	if err := json.Unmarshal(msg.Value, &data); err != nil {
		saveErr := wp.orphanRepo.Save(context.Background(), msg, lsid, timestamp, "", fmt.Sprintf("failed to parse message body: %v", err))
		if saveErr != nil {
			log.Printf("Failed to save orphaned message (parse error) for lsid=%d: %v", lsid, saveErr)
		}
		return fmt.Errorf("failed to parse message body: %w", err)
	}

	// Process each field
	for fieldName, fieldValue := range data {
		// Skip timestamp field as it's in headers
		if fieldName == "ts" {
			continue
		}

		// Lookup tag in cache
		tag := wp.cache.GetTag(device.ID, fieldName)

		if tag == nil {
			// Create tag on the fly with enrichment from catalog
			dataType := determineDataType(fieldValue)

			// Lookup field metadata from catalog
			var unit *string
			var description *string
			var metadata map[string]interface{}
			if sensorType > 0 && dataStructureType > 0 {
				if fieldMeta := wp.cache.GetCatalogMetadata(sensorType, dataStructureType, fieldName); fieldMeta != nil {
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

			tagID, err := wp.tagRepo.Create(context.Background(), device.ID, fieldName, dataType, unit, description, metadata)
			if err != nil {
				log.Printf("Failed to create tag %s: %v", fieldName, err)
				saveErr := wp.orphanRepo.Save(context.Background(), msg, lsid, timestamp, fieldName, fmt.Sprintf("failed_to_create_tag: %v", err))
				if saveErr != nil {
					log.Printf("Failed to save orphaned message (tag creation error) for lsid=%d, field=%s: %v", lsid, fieldName, saveErr)
				}
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

			wp.cache.SetTag(device.ID, fieldName, tag)
		}

		// Add record to batch writer
		if err := wp.addRecordToBatch(tag, fieldValue, timestamp); err != nil {
			log.Printf("Failed to add record for tag %s to batch: %v", fieldName, err)
		}
	}

	return nil
}

// addRecordToBatch adds a record to the appropriate batch
func (wp *WorkerPool) addRecordToBatch(tag *models.Tag, value interface{}, timestamp int64) error {
	if value == nil {
		return wp.batchWriter.AddNull(tag.ID, timestamp)
	}

	switch tag.DataType {
	case "numeric", "float", "integer":
		var numValue float64
		switch v := value.(type) {
		case float64:
			numValue = v
		case float32:
			numValue = float64(v)
		case int:
			numValue = float64(v)
		case int64:
			numValue = float64(v)
		default:
			return fmt.Errorf("unsupported numeric type for value: %T", value)
		}
		return wp.batchWriter.AddNumeric(tag.ID, numValue, timestamp)

	case "text", "string":
		strValue, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected string value, got %T", value)
		}
		return wp.batchWriter.AddText(tag.ID, strValue, timestamp)

	default:
		return fmt.Errorf("unknown data type: %s", tag.DataType)
	}
}

// Shutdown gracefully shuts down the worker pool
func (wp *WorkerPool) Shutdown(ctx context.Context) error {
	log.Println("Shutting down worker pool...")

	// Stop accepting new messages
	close(wp.messageChan)

	// Wait for workers to drain
	wp.wg.Wait()

	log.Println("All workers stopped")
	return nil
}

// GetMetrics returns current metrics
func (wp *WorkerPool) GetMetrics() (processed, errors int64) {
	wp.mutex.Lock()
	defer wp.mutex.Unlock()
	return wp.messagesProcessed, wp.processingErrors
}

// incrementProcessed increments the processed counter
func (wp *WorkerPool) incrementProcessed() {
	wp.mutex.Lock()
	defer wp.mutex.Unlock()
	wp.messagesProcessed++
}

// incrementErrors increments the error counter
func (wp *WorkerPool) incrementErrors() {
	wp.mutex.Lock()
	defer wp.mutex.Unlock()
	wp.processingErrors++
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
