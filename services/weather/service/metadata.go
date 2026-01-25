package service

import (
	"context"
	"encoding/json"
	"log"
	"strconv"

	"weather/internal"
	"weather/models"
)

// fetchSensorMetadata fetches sensor metadata from the API
func (s *Service) fetchSensorMetadata(ctx context.Context) error {
	response, err := s.apiClient.FetchSensorMetadata()
	if err != nil {
		return err
	}

	// Marshal to calculate hash
	body, err := json.Marshal(response)
	if err != nil {
		return err
	}

	// Check if metadata has changed
	hash := internal.CalculateHash(body)
	if s.lastMetadataHash["sensors"] == hash {
		log.Println("Sensor metadata unchanged, skipping publish")
		return nil
	}

	// Update sensor map AND dynamically track sensor types from API
	for i := range response.Sensors {
		sensor := &response.Sensors[i]
		s.sensorMap[sensor.LSID] = sensor
		s.sensorTypes[sensor.SensorType] = true

		// Publish each sensor to metadata topic
		if err := s.producer.Publish(ctx, "weather.metadata.sensors", sensor, map[string]string{
			"lsid":        strconv.Itoa(sensor.LSID),
			"sensor_type": strconv.Itoa(sensor.SensorType),
			"category":    sensor.Category,
			"station_id":  strconv.Itoa(sensor.StationID),
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

	filteredCatalog := models.SensorCatalogResponse{
		GeneratedAt:   response.GeneratedAt,
		SensorCatalog: make([]models.CatalogEntry, 0),
	}

	// Only include catalog entries for sensor types we actually have
	for _, entry := range response.SensorCatalog {
		if s.sensorTypes[entry.SensorType] {
			filteredCatalog.SensorCatalog = append(filteredCatalog.SensorCatalog, entry)
		}
	}

	sensorTypesList := getKeysFromMap(s.sensorTypes)
	log.Printf("Filtered catalog: %d/%d sensor types (kept: %v)",
		len(filteredCatalog.SensorCatalog), len(response.SensorCatalog), sensorTypesList)

	// Check if filtered catalog has changed
	filteredBody, _ := json.Marshal(filteredCatalog)
	hash := internal.CalculateHash(filteredBody)
	if s.lastMetadataHash["catalog"] == hash {
		log.Println("Filtered catalog unchanged, skipping publish")
		return nil
	}

	// Publish filtered catalog to metadata topic
	if err := s.producer.Publish(ctx, "weather.metadata.catalog", filteredCatalog, map[string]string{
		"entry_count": strconv.Itoa(len(filteredCatalog.SensorCatalog)),
	}); err != nil {
		return err
	}

	s.lastMetadataHash["catalog"] = hash
	log.Printf("Published filtered sensor catalog (%d types, ~%d bytes)",
		len(filteredCatalog.SensorCatalog), len(filteredBody))
	return nil
}

// fetchStationInfo fetches station information from the API
func (s *Service) fetchStationInfo(ctx context.Context) error {
	response, err := s.apiClient.FetchStationInfo()
	if err != nil {
		return err
	}

	// Marshal to calculate hash
	body, err := json.Marshal(response)
	if err != nil {
		return err
	}

	// Check if station info has changed
	hash := internal.CalculateHash(body)
	if s.lastMetadataHash["station"] == hash {
		log.Println("Station info unchanged, skipping publish")
		return nil
	}

	// Publish station info to metadata topic
	if err := s.producer.Publish(ctx, "weather.metadata.station", response, map[string]string{
		"station_id": s.config.WeatherLinkStationID,
	}); err != nil {
		return err
	}

	s.lastMetadataHash["station"] = hash
	log.Println("Published station info")
	return nil
}
