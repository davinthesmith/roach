package service

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"

	"weatherlink-kafka/api"
	"weatherlink-kafka/kafka"
	"weatherlink-kafka/models"
)

// Service manages the weather data collection
type Service struct {
	config               models.Config
	apiClient            *api.Client
	producer             *kafka.Producer
	db                   *sql.DB
	sensorMetadata       map[int]*models.SensorMetadata // by lsid
	sensorTypes          map[int]struct{}
	existingRecordKeys   map[string]struct{} // lsid:timestamp keys scanned from Kafka
	recordKeysMutex      sync.RWMutex
	existingMetadataKeys map[string]struct{} // metadata keys scanned from Kafka
	metadataKeysMutex    sync.RWMutex
}

// New creates a new Service
func New(cfg models.Config, apiClient *api.Client, producer *kafka.Producer, db *sql.DB) *Service {
	return &Service{
		config:               cfg,
		apiClient:            apiClient,
		producer:             producer,
		db:                   db,
		sensorMetadata:       make(map[int]*models.SensorMetadata),
		sensorTypes:          make(map[int]struct{}),
		existingRecordKeys:   make(map[string]struct{}),
		existingMetadataKeys: make(map[string]struct{}),
	}
}

// Start starts the weather service
func (s *Service) Start(ctx context.Context) error {
	// Scan Kafka for existing keys to prevent duplicates across restarts.
	if err := s.scanExistingKeys(ctx, []string{"weather.iss", "weather.barometer", "weather.indoor", "weather.health", "weather.other"}, s.existingRecordKeys, &s.recordKeysMutex); err != nil {
		log.Printf("Warning: Failed to scan Kafka for existing keys: %v", err)
	}

	if err := s.scanExistingKeys(ctx, []string{"weather.metadata.catalog", "weather.metadata.station", "weather.metadata.sensors"}, s.existingMetadataKeys, &s.metadataKeysMutex); err != nil {
		log.Printf("Warning: Failed to scan Kafka metadata keys: %v", err)
	}

	// Fetch metadata before starting loop
	s.fetchAllMetadata(ctx)

	// Start background goroutine for periodic metadata updates
	go s.metadataUpdateLoop(ctx)

	// Fetch current conditions before loop
	s.fetchAllCurrentConditions(ctx)

	// Start main loop for current conditions
	return s.currentConditionsUpdateLoop(ctx)
}

// metadataUpdateLoop runs periodic metadata updates
func (s *Service) metadataUpdateLoop(ctx context.Context) {
	ticker := time.NewTicker(s.config.MetadataFetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.fetchAllMetadata(ctx)
		}
	}
}

// currentConditionsUpdateLoop runs periodic current conditions updates
func (s *Service) currentConditionsUpdateLoop(ctx context.Context) error {
	ticker := time.NewTicker(s.config.FetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.fetchAllCurrentConditions(ctx)
		}
	}
}
