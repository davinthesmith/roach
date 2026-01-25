package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/roach/weatherlink-lib/api"
	"github.com/roach/weatherlink-lib/kafka"
	"github.com/roach/weatherlink-lib/models"
)

// Service manages the backfill process
type Service struct {
	apiClient    *api.Client
	producer     *kafka.Producer
	rateLimiter  *RateLimiter
	sensorMap    map[int]*models.SensorMetadata
	existingKeys map[string]bool // Track existing keys (lsid:timestamp)
	keysMutex    sync.RWMutex    // Thread-safe access
	maxWorkers   int             // Number of parallel workers
}

// New creates a new backfill service
func New(apiClient *api.Client, producer *kafka.Producer, requestsPerSecond int, maxWorkers int) *Service {
	if maxWorkers <= 0 {
		maxWorkers = 4 // Default
	}
	return &Service{
		apiClient:    apiClient,
		producer:     producer,
		rateLimiter:  NewRateLimiter(requestsPerSecond, 1000),
		sensorMap:    make(map[int]*models.SensorMetadata),
		existingKeys: make(map[string]bool),
		maxWorkers:   maxWorkers,
	}
}

// TimeWindow represents a 24-hour time window
type TimeWindow struct {
	Start int64
	End   int64
}

// Backfill performs historical data backfill with parallel window processing
func (s *Service) Backfill(ctx context.Context, startTs, endTs int64) error {
	log.Printf("Starting backfill: %s to %s",
		time.Unix(startTs, 0).Format("2006-01-02 15:04:05"),
		time.Unix(endTs, 0).Format("2006-01-02 15:04:05"))
	
	// Step 1: Fetch sensor metadata for topic routing
	if err := s.fetchSensorMetadata(ctx); err != nil {
		return fmt.Errorf("failed to fetch sensor metadata: %w", err)
	}
	
	// Step 2: Scan Kafka topics for existing keys
	topics := []string{"weather.iss", "weather.barometer", "weather.indoor", "weather.health"}
	log.Println("Scanning Kafka topics for existing messages...")
	if err := s.scanExistingKeys(ctx, topics); err != nil {
		return fmt.Errorf("failed to scan existing keys: %w", err)
	}
	log.Printf("Found %d existing message keys", len(s.existingKeys))
	
	// Step 3: Split time range into 24-hour windows
	windows := splitInto24HourWindows(startTs, endTs)
	log.Printf("Total windows: %d (24-hour each)", len(windows))
	
	// Step 4: Process windows in parallel with rate limiting
	// Use a worker pool to process multiple windows concurrently
	maxWorkers := s.maxWorkers
	if len(windows) < maxWorkers {
		maxWorkers = len(windows)
	}
	
	log.Printf("Processing with %d parallel workers", maxWorkers)
	
	type windowResult struct {
		windowNum int
		published int
		skipped   int
		err       error
	}
	
	windowChan := make(chan TimeWindow, len(windows))
	resultChan := make(chan windowResult, len(windows))
	
	// Start worker goroutines
	var wg sync.WaitGroup
	for w := 0; w < maxWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for window := range windowChan {
				select {
				case <-ctx.Done():
					return
				default:
				}
				
				// Apply rate limiting before API call
				if err := s.rateLimiter.Wait(ctx); err != nil {
					resultChan <- windowResult{err: err}
					continue
				}
				
				// Fetch historic data with retry logic
				data, err := s.fetchHistoricDataWithRetry(ctx, window.Start, window.End)
				if err != nil {
					log.Printf("Worker %d: Failed to fetch window: %v", workerID, err)
					resultChan <- windowResult{err: err}
					continue
				}
				
				// Record successful request
				s.rateLimiter.RecordRequest()
				
				// Process and publish data points (now async)
				published, skipped := s.processHistoricDataAsync(ctx, data)
				
				resultChan <- windowResult{
					published: published,
					skipped:   skipped,
				}
				
				log.Printf("Worker %d: Window %s to %s - Published %d, skipped %d",
					workerID,
					time.Unix(window.Start, 0).Format("2006-01-02 15:04:05"),
					time.Unix(window.End, 0).Format("2006-01-02 15:04:05"),
					published, skipped)
			}
		}(w)
	}
	
	// Send windows to workers
	go func() {
		for _, window := range windows {
			windowChan <- window
		}
		close(windowChan)
	}()
	
	// Wait for workers to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()
	
	// Collect results
	totalMessages := 0
	totalSkipped := 0
	errorCount := 0
	for result := range resultChan {
		if result.err != nil {
			if result.err == context.Canceled {
				log.Println("Backfill canceled by user")
				return result.err
			}
			errorCount++
			continue
		}
		totalMessages += result.published
		totalSkipped += result.skipped
	}
	
	// Flush all pending Kafka messages
	log.Println("Flushing pending Kafka messages...")
	remaining := s.producer.Flush(30000) // 30 second timeout
	if remaining > 0 {
		log.Printf("Warning: %d messages failed to flush", remaining)
	}
	
	if errorCount > 0 {
		log.Printf("Backfill completed with %d errors", errorCount)
	}
	
	log.Printf("Backfill complete: %d new messages published, %d duplicates skipped", totalMessages, totalSkipped)
	
	return nil
}

// fetchSensorMetadata fetches sensor metadata for topic routing
func (s *Service) fetchSensorMetadata(ctx context.Context) error {
	log.Println("Fetching sensor metadata for topic routing...")
	
	response, err := s.apiClient.FetchSensorMetadata()
	if err != nil {
		return err
	}
	
	// Build sensor map
	for i := range response.Sensors {
		sensor := &response.Sensors[i]
		s.sensorMap[sensor.LSID] = sensor
	}
	
	log.Printf("Found %d active sensors", len(s.sensorMap))
	return nil
}

// fetchHistoricDataWithRetry fetches historic data with retry logic
func (s *Service) fetchHistoricDataWithRetry(ctx context.Context, startTs, endTs int64) (*models.CurrentConditionsResponse, error) {
	maxRetries := 3
	
	for attempt := 1; attempt <= maxRetries; attempt++ {
		data, err := s.apiClient.FetchHistoricData(startTs, endTs)
		if err != nil {
			// Check if it's a rate limit error (simplified check)
			if strings.Contains(err.Error(), "429") {
				backoff := s.rateLimiter.RecordError(429)
				log.Printf("Attempt %d/%d failed (rate limit), retrying after %s...", 
					attempt, maxRetries, backoff)
				
				select {
				case <-time.After(backoff):
					continue
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			
			// Other errors
			if attempt < maxRetries {
				backoff := time.Second * time.Duration(attempt)
				log.Printf("Attempt %d/%d failed: %v, retrying after %s...", 
					attempt, maxRetries, err, backoff)
				
				select {
				case <-time.After(backoff):
					continue
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			
			return nil, fmt.Errorf("failed after %d attempts: %w", maxRetries, err)
		}
		
		return data, nil
	}
	
	return nil, fmt.Errorf("failed after %d attempts", maxRetries)
}

// processHistoricData processes and publishes historic data points synchronously (for backward compatibility)
func (s *Service) processHistoricData(ctx context.Context, data *models.CurrentConditionsResponse) (int, int) {
	messagesPublished := 0
	messagesSkipped := 0
	
	for _, sensor := range data.Sensors {
		// Get sensor metadata for topic routing
		metadata, exists := s.sensorMap[sensor.LSID]
		if !exists {
			log.Printf("Warning: No metadata found for sensor %d, skipping", sensor.LSID)
			continue
		}
		
		// Determine topic based on category
		topic := getTopicForCategory(metadata.Category)
		
		// Publish each data point
		for _, dataPoint := range sensor.Data {
			// Extract timestamp from data point
			var dataMap map[string]interface{}
			if err := json.Unmarshal(dataPoint, &dataMap); err != nil {
				log.Printf("Failed to parse data point: %v", err)
				continue
			}
			
			timestamp := int64(0)
			if ts, ok := dataMap["ts"].(float64); ok {
				timestamp = int64(ts)
			}
			
			// Generate unique message key using lsid:timestamp
			key := strconv.Itoa(sensor.LSID) + ":" + strconv.FormatInt(timestamp, 10)
			
			// Check if key already exists
			s.keysMutex.RLock()
			exists := s.existingKeys[key]
			s.keysMutex.RUnlock()
			
			if exists {
				messagesSkipped++
				continue // Skip this message
			}
			
			// Create headers (matching ingest service format)
			headers := map[string]string{
				"schema_version":      "1",
				"lsid":                strconv.Itoa(sensor.LSID),
				"timestamp":           strconv.FormatInt(timestamp, 10),
				"sensor_type":         strconv.Itoa(sensor.SensorType),
				"data_structure_type": strconv.Itoa(sensor.DataStructureType),
			}
			
			// Publish to Kafka
			if err := s.producer.Publish(ctx, topic, key, dataPoint, headers); err != nil {
				log.Printf("Failed to publish %s: %v", key, err)
				// Continue processing other messages
			} else {
				messagesPublished++
				
				// Add to cache after successful publish
				s.keysMutex.Lock()
				s.existingKeys[key] = true
				s.keysMutex.Unlock()
			}
		}
	}
	
	return messagesPublished, messagesSkipped
}

// processHistoricDataAsync processes and publishes historic data points asynchronously
// This is much faster as it doesn't wait for each message delivery
func (s *Service) processHistoricDataAsync(ctx context.Context, data *models.CurrentConditionsResponse) (int, int) {
	messagesPublished := 0
	messagesSkipped := 0
	
	for _, sensor := range data.Sensors {
		// Get sensor metadata for topic routing
		metadata, exists := s.sensorMap[sensor.LSID]
		if !exists {
			log.Printf("Warning: No metadata found for sensor %d, skipping", sensor.LSID)
			continue
		}
		
		// Determine topic based on category
		topic := getTopicForCategory(metadata.Category)
		
		// Publish each data point asynchronously
		for _, dataPoint := range sensor.Data {
			// Extract timestamp from data point
			var dataMap map[string]interface{}
			if err := json.Unmarshal(dataPoint, &dataMap); err != nil {
				log.Printf("Failed to parse data point: %v", err)
				continue
			}
			
			timestamp := int64(0)
			if ts, ok := dataMap["ts"].(float64); ok {
				timestamp = int64(ts)
			}
			
			// Generate unique message key using lsid:timestamp
			key := strconv.Itoa(sensor.LSID) + ":" + strconv.FormatInt(timestamp, 10)
			
			// Check if key already exists
			s.keysMutex.RLock()
			exists := s.existingKeys[key]
			s.keysMutex.RUnlock()
			
			if exists {
				messagesSkipped++
				continue // Skip this message
			}
			
			// Create headers (matching ingest service format)
			headers := map[string]string{
				"schema_version":      "1",
				"lsid":                strconv.Itoa(sensor.LSID),
				"timestamp":           strconv.FormatInt(timestamp, 10),
				"sensor_type":         strconv.Itoa(sensor.SensorType),
				"data_structure_type": strconv.Itoa(sensor.DataStructureType),
			}
			
			// Publish to Kafka asynchronously (don't wait for delivery)
			if err := s.producer.PublishAsync(ctx, topic, key, dataPoint, headers); err != nil {
				log.Printf("Failed to publish %s: %v", key, err)
				// Continue processing other messages
			} else {
				messagesPublished++
				
				// Add to cache immediately (optimistic - assume success)
				s.keysMutex.Lock()
				s.existingKeys[key] = true
				s.keysMutex.Unlock()
			}
		}
	}
	
	return messagesPublished, messagesSkipped
}

// splitInto24HourWindows splits a time range into 24-hour windows
func splitInto24HourWindows(startTs, endTs int64) []TimeWindow {
	const windowSize = 86400 // 24 hours in seconds
	
	windows := []TimeWindow{}
	currentStart := startTs
	
	for currentStart < endTs {
		currentEnd := currentStart + windowSize
		if currentEnd > endTs {
			currentEnd = endTs
		}
		
		windows = append(windows, TimeWindow{
			Start: currentStart,
			End:   currentEnd,
		})
		
		currentStart = currentEnd
	}
	
	return windows
}

// getTopicForCategory determines the Kafka topic based on sensor category
func getTopicForCategory(category string) string {
	switch strings.ToUpper(category) {
	case "ISS":
		return "weather.iss"
	case "BAROMETER":
		return "weather.barometer"
	case "INSIDE TEMP/HUM":
		return "weather.indoor"
	case "HEALTH":
		return "weather.health"
	default:
		log.Printf("Unknown category '%s', using default topic", category)
		return "weather.other"
	}
}
