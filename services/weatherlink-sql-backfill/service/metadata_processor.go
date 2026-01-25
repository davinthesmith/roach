package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"

	"weatherlink-sql-backfill/cache"
	"weatherlink-sql-backfill/models"
	"weatherlink-sql-backfill/repository"
)

// MetadataProcessor handles metadata message processing for backfill
type MetadataProcessor struct {
	deviceRepo *repository.DeviceRepository
	orphanRepo *repository.OrphanRepository
	cache      *cache.Cache
}

// NewMetadataProcessor creates a new MetadataProcessor
func NewMetadataProcessor(deviceRepo *repository.DeviceRepository, orphanRepo *repository.OrphanRepository, cache *cache.Cache) *MetadataProcessor {
	return &MetadataProcessor{
		deviceRepo: deviceRepo,
		orphanRepo: orphanRepo,
		cache:      cache,
	}
}

// ProcessMessage processes a metadata message (device/station)
func (p *MetadataProcessor) ProcessMessage(ctx context.Context, msg kafka.Message) error {
	var metadata map[string]interface{}
	if err := json.Unmarshal(msg.Value, &metadata); err != nil {
		// Save orphaned message with parse error
		p.orphanRepo.Save(ctx, msg, 0, 0, "", fmt.Sprintf("failed to parse metadata: %v", err))
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	// Check if this is a station metadata message (has "stations" array)
	if stations, ok := metadata["stations"].([]interface{}); ok {
		return p.processStationMetadata(ctx, msg, stations)
	}

	// Otherwise, process as device metadata (has "lsid")
	lsid, ok := metadata["lsid"].(float64)
	if !ok {
		// Save orphaned message with missing lsid
		p.orphanRepo.Save(ctx, msg, 0, 0, "", "missing or invalid lsid in metadata")
		return fmt.Errorf("missing or invalid lsid in metadata")
	}

	// Upsert device
	if err := p.deviceRepo.Upsert(ctx, metadata); err != nil {
		// Save orphaned message with upsert error
		p.orphanRepo.Save(ctx, msg, int(lsid), 0, "", fmt.Sprintf("failed to upsert device: %v", err))
		return fmt.Errorf("failed to upsert device: %w", err)
	}

	log.Printf("Upserted device %d", int(lsid))

	// Update cache immediately (avoid full reload for performance)
	device := p.extractDeviceFromMetadata(metadata)
	if device != nil {
		p.cache.SetDevice(device.LSID, device)
	}

	return nil
}

// processStationMetadata processes station-level metadata that applies to multiple devices
func (p *MetadataProcessor) processStationMetadata(ctx context.Context, msg kafka.Message, stations []interface{}) error {
	if len(stations) == 0 {
		return fmt.Errorf("stations array is empty")
	}

	// Process each station (typically just one)
	for _, stationInterface := range stations {
		station, ok := stationInterface.(map[string]interface{})
		if !ok {
			log.Printf("Warning: Invalid station object in stations array")
			continue
		}

		stationID, hasStationID := station["station_id"].(float64)
		stationName, _ := station["station_name"].(string)
		stationUUID, _ := station["station_id_uuid"].(string)

		if !hasStationID {
			log.Printf("Warning: Station missing station_id")
			continue
		}

		log.Printf("Processing station metadata: station_id=%d, name=%s", int(stationID), stationName)

		// Update all devices that belong to this station
		if err := p.deviceRepo.UpdateStationInfo(ctx, int(stationID), stationName, stationUUID); err != nil {
			// Save orphaned message with update error
			p.orphanRepo.Save(ctx, msg, 0, 0, "", fmt.Sprintf("failed to update station info for station_id %d: %v", int(stationID), err))
			return fmt.Errorf("failed to update station info: %w", err)
		}

		log.Printf("Updated station info for station_id=%d (%s)", int(stationID), stationName)
	}

	return nil
}

// extractDeviceFromMetadata extracts a Device model from metadata map
func (p *MetadataProcessor) extractDeviceFromMetadata(metadata map[string]interface{}) *models.Device {
	lsid, ok := metadata["lsid"].(float64)
	if !ok {
		return nil
	}

	device := &models.Device{
		LSID: int(lsid),
	}

	if sensorType, ok := metadata["sensor_type"].(float64); ok {
		device.SensorType = int(sensorType)
	}

	if category, ok := metadata["category"].(string); ok {
		device.Category = category
	}

	if manufacturer, ok := metadata["manufacturer"].(string); ok {
		device.Manufacturer = manufacturer
	}

	if productName, ok := metadata["product_name"].(string); ok {
		device.ProductName = productName
	}

	// Note: rt_data_structure_type may not be in metadata message, that's ok
	// It will be populated from actual data messages

	return device
}
