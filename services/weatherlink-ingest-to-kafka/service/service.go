package service

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/roach/weatherlink-lib/api"
	"github.com/roach/weatherlink-lib/kafka"
	"github.com/roach/weatherlink-lib/models"
)

// Service manages the weather data collection
type Service struct {
	config           models.Config
	apiClient        *api.Client
	producer         *kafka.Producer
	db               *sql.DB
	sensorMap        map[int]*models.SensorMetadata
	sensorTypes      map[int]bool
	lastMetadataHash map[string]string
	timestampCache   map[int]map[int]int64 // LSID → data_structure_type → timestamp
	cacheMutex       sync.RWMutex
}

// New creates a new Service
func New(cfg models.Config, apiClient *api.Client, producer *kafka.Producer, db *sql.DB) *Service {
	return &Service{
		config:           cfg,
		apiClient:        apiClient,
		producer:         producer,
		db:               db,
		sensorMap:        make(map[int]*models.SensorMetadata),
		sensorTypes:      make(map[int]bool),
		lastMetadataHash: make(map[string]string),
		timestampCache:   make(map[int]map[int]int64),
	}
}

// Start starts the weather service
func (s *Service) Start(ctx context.Context) error {
	// Connect to PostgreSQL if configured and rehydrate cache
	if s.db != nil {
		if err := s.rehydrateCache(ctx); err != nil {
			log.Printf("Warning: Failed to rehydrate cache: %v", err)
		}
	}

	// Fetch metadata on startup - ORDER MATTERS!
	// 1. Sensors first (to populate sensorTypes filter)
	if err := s.fetchSensorMetadata(ctx); err != nil {
		log.Printf("Warning: Failed to fetch sensor metadata: %v", err)
	}

	// 2. Catalog second (uses sensorTypes filter)
	if err := s.fetchSensorCatalog(ctx); err != nil {
		log.Printf("Warning: Failed to fetch sensor catalog: %v", err)
	}

	// 3. Station info last
	if err := s.fetchStationInfo(ctx); err != nil {
		log.Printf("Warning: Failed to fetch station info: %v", err)
	}

	// Start background goroutine for daily metadata updates
	go s.metadataUpdateLoop(ctx)

	// Start main loop for current conditions
	ticker := time.NewTicker(s.config.FetchInterval)
	defer ticker.Stop()

	// Fetch immediately on start
	if err := s.fetchCurrentConditions(ctx); err != nil {
		log.Printf("Error fetching current conditions: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.fetchCurrentConditions(ctx); err != nil {
				log.Printf("Error fetching current conditions: %v", err)
			}
		}
	}
}

// metadataUpdateLoop runs periodic metadata updates
func (s *Service) metadataUpdateLoop(ctx context.Context) {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Println("Daily metadata update...")

			if err := s.fetchSensorMetadata(ctx); err != nil {
				log.Printf("Error updating sensor metadata: %v", err)
			}

			if err := s.fetchSensorCatalog(ctx); err != nil {
				log.Printf("Error updating sensor catalog: %v", err)
			}

			if err := s.fetchStationInfo(ctx); err != nil {
				log.Printf("Error updating station info: %v", err)
			}
		}
	}
}

// getTopicForCategory determines the Kafka topic based on sensor category
func (s *Service) getTopicForCategory(category string) string {
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

// checkDuplicate checks if a message with the same timestamp was already published
// Now accounts for data_structure_type to handle sensors with multiple data structures
func (s *Service) checkDuplicate(lsid int, dataStructureType int, timestamp int64) bool {
	s.cacheMutex.RLock()
	defer s.cacheMutex.RUnlock()
	
	if structTypes, exists := s.timestampCache[lsid]; exists {
		if lastTimestamp, exists := structTypes[dataStructureType]; exists {
			return timestamp == lastTimestamp
		}
	}
	return false
}

// updateCache updates the timestamp cache
func (s *Service) updateCache(lsid int, dataStructureType int, timestamp int64) {
	s.cacheMutex.Lock()
	defer s.cacheMutex.Unlock()
	
	if s.timestampCache[lsid] == nil {
		s.timestampCache[lsid] = make(map[int]int64)
	}
	s.timestampCache[lsid][dataStructureType] = timestamp
}

// getKeysFromMap returns the keys from a map[int]bool
func getKeysFromMap(m map[int]bool) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
