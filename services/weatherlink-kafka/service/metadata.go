package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"weatherlink-kafka/models"
	"weatherlink-kafka/util"
)

// fetchSensorMetadata fetches sensor metadata from the API
func (s *Service) fetchSensorMetadata(ctx context.Context) error {
	response, err := s.apiClient.FetchSensorMetadata()
	if err != nil {
		return err
	}

	// Marshal to calculate hash (ignore generated_at to avoid duplicates)
	hashPayload := struct {
		Sensors []models.SensorMetadata `json:"sensors"`
	}{
		Sensors: response.Sensors,
	}

	body, err := json.Marshal(hashPayload)
	if err != nil {
		return err
	}

	// Check if metadata has changed
	hash := util.CalculateHash(body)
	if s.lastMetadataHash["sensors"] == hash {
		log.Println("Sensor metadata unchanged, skipping publish")
		return nil
	}

	// Update sensor map AND dynamically track sensor types from API
	for i := range response.Sensors {
		sensor := &response.Sensors[i]
		s.sensorMap[sensor.LSID] = sensor
		s.sensorTypes[sensor.SensorType] = true

		// Generate unique key for sensor metadata: lsid:{lsid}
		key := "lsid:" + strconv.Itoa(sensor.LSID)

		// Publish each sensor to metadata topic
		if err := s.producer.Publish(ctx, "weather.metadata.sensors", key, sensor, map[string]string{
			"schema_version": "1",
		}); err != nil {
			log.Printf("Failed to publish sensor metadata: %v", err)
		}
	}

	s.lastMetadataHash["sensors"] = hash
	log.Printf("Published metadata for %d sensors (sensor types: %v)",
		len(response.Sensors), getKeysFromMap(s.sensorTypes))
	return nil
}

// fetchSensorCatalog fetches the sensor catalog from the API
func (s *Service) fetchSensorCatalog(ctx context.Context) error {
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
		if s.sensorTypes[entry.SensorType] {
			filteredEntries = append(filteredEntries, entry)
		}
	}

	if len(filteredEntries) == 0 {
		log.Println("No catalog entries match our sensor types, skipping publish")
		return nil
	}

	sensorTypesList := getKeysFromMap(s.sensorTypes)
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

		s.catalogMutex.RLock()
		_, exists := s.existingCatalogKeys[key]
		s.catalogMutex.RUnlock()
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

		s.catalogMutex.Lock()
		s.existingCatalogKeys[key] = struct{}{}
		s.catalogMutex.Unlock()
	}

	if skippedCount > 0 {
		log.Printf("Published %d catalog entries, skipped %d duplicates", publishedCount, skippedCount)
	} else {
		log.Printf("Published %d catalog entries as separate messages", publishedCount)
	}
	return nil
}

// fetchStationInfo fetches station information from the API
func (s *Service) fetchStationInfo(ctx context.Context) error {
	response, err := s.apiClient.FetchStationInfo()
	if err != nil {
		return err
	}

	// Marshal to calculate hash (ignore generated_at to avoid duplicates)
	hashPayload := struct {
		Stations []models.StationInfo `json:"stations"`
	}{
		Stations: response.Stations,
	}

	body, err := json.Marshal(hashPayload)
	if err != nil {
		return err
	}

	// Check if station info has changed
	hash := util.CalculateHash(body)
	if s.lastMetadataHash["station"] == hash {
		log.Println("Station info unchanged, skipping publish")
		return nil
	}

	// Generate key for station info: station:{station_id}
	key := "station:" + s.config.WeatherLinkStationID

	// Publish station info to metadata topic
	if err := s.producer.Publish(ctx, "weather.metadata.station", key, response, map[string]string{
		"schema_version": "1",
		"station_id":     s.config.WeatherLinkStationID,
	}); err != nil {
		return err
	}

	s.lastMetadataHash["station"] = hash
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
