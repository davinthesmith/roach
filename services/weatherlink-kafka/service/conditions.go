package service

import (
	"context"
	"encoding/json"
	"log"
	"strconv"

	"weatherlink-kafka/util"
)

// fetchAllCurrentConditions runs the metadata update flow with logs matching the update loop.
func (s *Service) fetchAllCurrentConditions(ctx context.Context) {
	log.Println("Current Conditions update...")

	if err := s.fetchCurrentConditions(ctx); err != nil {
		log.Printf("Error fetching current conditions: %v", err)
	}
}

// fetchCurrentConditions fetches current conditions from the API
func (s *Service) fetchCurrentConditions(ctx context.Context) error {
	response, err := s.apiClient.FetchCurrentConditions()
	if err != nil {
		return err
	}

	messagesPublished := 0
	messagesSkipped := 0
	seenKeys := make(map[string]struct{})

	for _, sensor := range response.Sensors {
		// Get sensor metadata
		metadata, exists := s.sensorMetadata[sensor.LSID]
		if !exists {
			log.Printf("Warning: No metadata found for sensor %d, skipping", sensor.LSID)
			continue
		}

		// Determine topic based on category
		topic := util.GetTopicForCategory(metadata.Category)

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
			if _, exists := seenKeys[key]; exists {
				messagesSkipped++
				continue
			}

			// Skip if key already exists in Kafka (dedup across restarts).
			s.recordKeysMutex.RLock()
			_, exists := s.existingRecordKeys[key]
			s.recordKeysMutex.RUnlock()
			if exists {
				messagesSkipped++
				continue
			}

			seenKeys[key] = struct{}{}

			// Optimized headers: removed redundant static/metadata fields
			// station_id, station_id_uuid, category, product_name available via metadata lookup
			headers := map[string]string{
				"schema_version":      "1",
				"lsid":                strconv.Itoa(sensor.LSID),
				"timestamp":           strconv.FormatInt(timestamp, 10),
				"sensor_type":         strconv.Itoa(sensor.SensorType),
				"data_structure_type": strconv.Itoa(sensor.DataStructureType),
			}

			if err := s.producer.Publish(ctx, topic, key, dataPoint, headers); err != nil {
				log.Printf("Failed to publish data point: %v", err)
			} else {
				messagesPublished++

				s.recordKeysMutex.Lock()
				s.existingRecordKeys[key] = struct{}{}
				s.recordKeysMutex.Unlock()

			}
		}
	}

	if messagesSkipped > 0 {
		log.Printf("Published %d sensor records, skipped %d duplicates", messagesPublished, messagesSkipped)
	} else {
		log.Printf("Published %d sensor records", messagesPublished)
	}
	return nil
}
