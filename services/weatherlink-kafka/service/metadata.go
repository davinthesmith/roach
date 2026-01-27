package service

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"weatherlink-kafka/models"
	"weatherlink-kafka/util"
)

// fetchAllMetadata runs the metadata update flow with logs matching the update loop.
func (s *Service) fetchAllMetadata(ctx context.Context) {
	log.Println("Metadata update...")

	if err := s.fetchSensorMetadata(ctx); err != nil {
		log.Printf("Error updating sensor metadata: %v", err)
	}

	if err := s.fetchCatalogMetadata(ctx); err != nil {
		log.Printf("Error updating sensor catalog: %v", err)
	}

	if err := s.fetchStationMetadata(ctx); err != nil {
		log.Printf("Error updating station info: %v", err)
	}
}

// fetchSensorMetadata fetches sensor metadata from the API
func (s *Service) fetchSensorMetadata(ctx context.Context) error {
	response, err := s.apiClient.FetchSensorMetadata()
	if err != nil {
		return err
	}

	weekStart := util.StartOfWeekUnix(time.Now().UTC())

	// Update sensor map AND dynamically track sensor types from API
	for i := range response.Sensors {
		sensor := &response.Sensors[i]
		s.sensorMap[sensor.LSID] = sensor
		s.sensorTypes[sensor.SensorType] = struct{}{}

		// Generate unique key for sensor metadata: {lsid}:{weekStart}
		key := fmt.Sprintf("%d:%d", sensor.LSID, weekStart)

		// Skip if key already exists in Kafka (dedup across restarts).
		s.metadataKeysMutex.RLock()
		_, exists := s.existingMetadataKeys[key]
		s.metadataKeysMutex.RUnlock()
		if exists {
			continue
		}

		// Publish each sensor to metadata topic
		if err := s.producer.Publish(ctx, "weather.metadata.sensors", key, sensor, map[string]string{
			"schema_version": "1",
		}); err != nil {
			log.Printf("Failed to publish sensor metadata: %v", err)
			continue
		}

		s.metadataKeysMutex.Lock()
		s.existingMetadataKeys[key] = struct{}{}
		s.metadataKeysMutex.Unlock()
	}

	log.Printf("Published metadata for %d sensors (sensor types: %v)",
		len(response.Sensors), util.KeysFromSet(s.sensorTypes))
	return nil
}

// fetchCatalogMetadata fetches the sensor catalog from the API
func (s *Service) fetchCatalogMetadata(ctx context.Context) error {
	response, err := s.apiClient.FetchSensorCatalog()
	if err != nil {
		return err
	}

	// Filter catalog to only include sensor types we actually have
	if len(s.sensorTypes) == 0 {
		log.Println("No sensor types discovered yet from sensors API, skipping catalog publish")
		return nil
	}

	// Filter and collect catalog entries for sensor types we actually have
	var filteredEntries []models.CatalogEntry
	for _, entry := range response.SensorCatalog {
		if _, ok := s.sensorTypes[entry.SensorType]; ok {
			filteredEntries = append(filteredEntries, entry)
		}
	}

	if len(filteredEntries) == 0 {
		log.Println("No catalog entries match our sensor types, skipping publish")
		return nil
	}

	sensorTypesList := util.KeysFromSet(s.sensorTypes)
	log.Printf("Filtered catalog: %d/%d sensor types (kept: %v)",
		len(filteredEntries), len(response.SensorCatalog), sensorTypesList)

	// Publish each catalog entry as a separate message (avoids large message issues)
	// This allows consumers to process incrementally and avoids Kafka size limits
	publishedCount := 0
	skippedCount := 0
	for _, entry := range filteredEntries {
		maxStructureType, ok := maxDataStructureType(entry)
		if !ok {
			log.Printf("No valid data_structure_type found for sensor_type %d, skipping", entry.SensorType)
			continue
		}
		key := fmt.Sprintf("%d:%d", entry.SensorType, maxStructureType)

		s.metadataKeysMutex.RLock()
		_, exists := s.existingMetadataKeys[key]
		s.metadataKeysMutex.RUnlock()
		if exists {
			skippedCount++
			continue
		}

		headers := map[string]string{
			"schema_version": "1",
			"generated_at":   strconv.FormatInt(response.GeneratedAt, 10),
		}

		if err := s.producer.Publish(ctx, "weather.metadata.catalog", key, entry, headers); err != nil {
			log.Printf("Failed to publish catalog entry for sensor_type %d: %v", entry.SensorType, err)
			continue
		}
		publishedCount++

		s.metadataKeysMutex.Lock()
		s.existingMetadataKeys[key] = struct{}{}
		s.metadataKeysMutex.Unlock()
	}

	if skippedCount > 0 {
		log.Printf("Published %d catalog entries, skipped %d duplicates", publishedCount, skippedCount)
	} else {
		log.Printf("Published %d catalog entries as separate messages", publishedCount)
	}
	return nil
}

// fetchStationMetadata fetches station information from the API
func (s *Service) fetchStationMetadata(ctx context.Context) error {
	response, err := s.apiClient.FetchStationInfo()
	if err != nil {
		return err
	}

	weekStart := util.StartOfWeekUnix(time.Now().UTC())

	// Generate key for station info: {station_id}:{weekStart}
	key := fmt.Sprintf("%s:%d", s.config.WeatherLinkStationID, weekStart)

	// Skip if key already exists in Kafka (dedup across restarts).
	s.metadataKeysMutex.RLock()
	_, exists := s.existingMetadataKeys[key]
	s.metadataKeysMutex.RUnlock()
	if exists {
		return nil
	}

	// Publish station info to metadata topic
	if err := s.producer.Publish(ctx, "weather.metadata.station", key, response, map[string]string{
		"schema_version": "1",
		"station_id":     s.config.WeatherLinkStationID,
	}); err != nil {
		return err
	}

	s.metadataKeysMutex.Lock()
	s.existingMetadataKeys[key] = struct{}{}
	s.metadataKeysMutex.Unlock()

	log.Println("Published station info")
	return nil
}

func maxDataStructureType(entry models.CatalogEntry) (int, bool) {
	maxValue := 0
	found := false
	for _, dataStructure := range entry.DataStructures {
		value, err := strconv.Atoi(dataStructure.DataStructureType)
		if err != nil {
			continue
		}
		if !found || value > maxValue {
			maxValue = value
			found = true
		}
	}
	return maxValue, found
}
