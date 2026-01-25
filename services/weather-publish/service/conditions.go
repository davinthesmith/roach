package service

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
)

// fetchCurrentConditions fetches current conditions from the API
func (s *Service) fetchCurrentConditions(ctx context.Context) error {
	response, err := s.apiClient.FetchCurrentConditions()
	if err != nil {
		return err
	}

	messagesPublished := 0
	messagesSkipped := 0

	for _, sensor := range response.Sensors {
		// Get sensor metadata
		metadata, exists := s.sensorMap[sensor.LSID]
		if !exists {
			log.Printf("Warning: No metadata found for sensor %d, skipping", sensor.LSID)
			continue
		}

		// Determine topic based on category
		topic := s.getTopicForCategory(metadata.Category)

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

		// Check if we've already published this timestamp for this sensor
		if timestamp > 0 && s.checkDuplicate(sensor.LSID, timestamp) {
			messagesSkipped++
			continue
		}

		// Generate unique message key using lsid:timestamp
		key := strconv.Itoa(sensor.LSID) + ":" + strconv.FormatInt(timestamp, 10)

		headers := map[string]string{
			"lsid":                strconv.Itoa(sensor.LSID),
			"timestamp":           strconv.FormatInt(timestamp, 10),
			"station_id":          strconv.Itoa(response.StationID),
			"station_id_uuid":     response.StationIDUUID,
			"sensor_type":         strconv.Itoa(sensor.SensorType),
			"data_structure_type": strconv.Itoa(sensor.DataStructureType),
			"category":            metadata.Category,
			"product_name":        metadata.ProductName,
		}

		if err := s.producer.Publish(ctx, topic, key, dataPoint, headers); err != nil {
			log.Printf("Failed to publish data point: %v", err)
		} else {
			messagesPublished++

			// Update timestamp cache
			if timestamp > 0 {
				s.updateCache(sensor.LSID, timestamp)
			}
		}
		}
	}

	if messagesSkipped > 0 {
		log.Printf("Published %d sensor readings, skipped %d duplicates", messagesPublished, messagesSkipped)
	} else {
		log.Printf("Published %d sensor readings", messagesPublished)
	}
	return nil
}
