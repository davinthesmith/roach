package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"weather-sql/cache"
	"weather-sql/repository"
)

// MetadataProcessor handles metadata message processing
type MetadataProcessor struct {
	deviceRepo *repository.DeviceRepository
	cache      *cache.Cache
}

// NewMetadataProcessor creates a new MetadataProcessor
func NewMetadataProcessor(deviceRepo *repository.DeviceRepository, cache *cache.Cache) *MetadataProcessor {
	return &MetadataProcessor{
		deviceRepo: deviceRepo,
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
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	lsid, ok := metadata["lsid"].(float64)
	if !ok {
		return fmt.Errorf("missing or invalid lsid in metadata")
	}

	// Upsert device
	if err := p.deviceRepo.Upsert(ctx, metadata); err != nil {
		return fmt.Errorf("failed to upsert device: %w", err)
	}

	log.Printf("Upserted device %d", int(lsid))

	// Reload devices to refresh cache
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
