package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"weatherlink-sql/cache"
	"weatherlink-sql/repository"
)

// MetadataProcessor handles metadata message processing
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

// LoadDevices loads devices from database into cache
func (p *MetadataProcessor) LoadDevices(ctx context.Context) error {
	devices, err := p.deviceRepo.LoadAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to load devices: %w", err)
	}

	for _, device := range devices {
		p.cache.SetDevice(device.LSID, device)
	}

	log.Printf("Loaded %d devices into cache", len(devices))
	return nil
}

// ProcessMessage processes a metadata message
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

	// Reload devices to refresh cache
	return p.LoadDevices(ctx)
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

	// Reload devices to refresh cache with updated station info
	return p.LoadDevices(ctx)
}

// Listen listens for metadata messages
func (p *MetadataProcessor) Listen(ctx context.Context, reader *kafka.Reader) {
	log.Println("Starting metadata listener...")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				log.Printf("Error fetching metadata message: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if err := p.ProcessMessage(ctx, msg); err != nil {
				log.Printf("Error processing metadata message: %v", err)
			}

			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Error committing metadata message: %v", err)
			}
		}
	}
}
