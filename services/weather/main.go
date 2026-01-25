package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

// Configuration
type Config struct {
	WeatherLinkAPIKey    string
	WeatherLinkAPISecret string
	WeatherLinkStationID string
	KafkaBroker          string
	PostgresDSN          string
	FetchInterval        time.Duration
	LogLevel             string
}

// WeatherLink API Response Structures
type CurrentConditionsResponse struct {
	StationID     int      `json:"station_id"`
	StationIDUUID string   `json:"station_id_uuid"`
	Sensors       []Sensor `json:"sensors"`
	GeneratedAt   int64    `json:"generated_at"`
}

type Sensor struct {
	LSID              int               `json:"lsid"`
	SensorType        int               `json:"sensor_type"`
	DataStructureType int               `json:"data_structure_type"`
	Data              []json.RawMessage `json:"data"`
}

type SensorsResponse struct {
	Sensors     []SensorMetadata `json:"sensors"`
	GeneratedAt int64            `json:"generated_at"`
}

type SensorMetadata struct {
	LSID              int     `json:"lsid"`
	SensorType        int     `json:"sensor_type"`
	Category          string  `json:"category"`
	Manufacturer      string  `json:"manufacturer"`
	ProductName       string  `json:"product_name"`
	StationID         int     `json:"station_id"`
	StationIDUUID     string  `json:"station_id_uuid"`
	StationName       string  `json:"station_name"`
	Latitude          float64 `json:"latitude"`
	Longitude         float64 `json:"longitude"`
	Elevation         float64 `json:"elevation"`
}

type StationResponse struct {
	Stations    []StationInfo `json:"stations"`
	GeneratedAt int64         `json:"generated_at"`
}

type StationInfo struct {
	StationID     int    `json:"station_id"`
	StationIDUUID string `json:"station_id_uuid"`
	StationName   string `json:"station_name"`
	// Add more fields as needed
}

type SensorCatalogResponse struct {
	SensorCatalog []CatalogEntry `json:"sensor_types"`
	GeneratedAt   int64          `json:"generated_at"`
}

type CatalogEntry struct {
	SensorType   int    `json:"sensor_type"`
	Manufacturer string `json:"manufacturer"`
	ProductName  string `json:"product_name"`
	Category     string `json:"category"`
	// Add more fields as needed
}

// Service manages the weather data collection
type Service struct {
	config           Config
	kafkaWriter      *kafka.Writer
	sensorMap        map[int]SensorMetadata
	lastMetadataHash map[string]string
	httpClient       *http.Client
	db               *sql.DB
	timestampCache   map[int]int64
	cacheMutex       sync.RWMutex
}

func loadConfig() Config {
	fetchInterval := os.Getenv("FETCH_INTERVAL")
	if fetchInterval == "" {
		fetchInterval = "5m"
	}
	duration, err := time.ParseDuration(fetchInterval)
	if err != nil {
		log.Fatalf("Invalid FETCH_INTERVAL: %v", err)
	}

	return Config{
		WeatherLinkAPIKey:    os.Getenv("WEATHERLINK_API_KEY"),
		WeatherLinkAPISecret: os.Getenv("WEATHERLINK_API_SECRET"),
		WeatherLinkStationID: os.Getenv("WEATHERLINK_STATION_ID"),
		KafkaBroker:          os.Getenv("KAFKA_BROKER"),
		PostgresDSN:          os.Getenv("POSTGRES_DSN"),
		FetchInterval:        duration,
		LogLevel:             getEnvOrDefault("LOG_LEVEL", "info"),
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func NewService(config Config) *Service {
	return &Service{
		config: config,
		kafkaWriter: &kafka.Writer{
			Addr:                   kafka.TCP(config.KafkaBroker),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
			Async:                  false,
		},
		sensorMap:        make(map[int]SensorMetadata),
		lastMetadataHash: make(map[string]string),
		timestampCache:   make(map[int]int64),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (s *Service) generateAPISignature(params map[string]string) string {
	// Sort parameters and create signature string
	var sortedParams []string
	for key, value := range params {
		sortedParams = append(sortedParams, key+value)
	}
	
	// WeatherLink v2 API uses HMAC-SHA256
	h := hmac.New(sha256.New, []byte(s.config.WeatherLinkAPISecret))
	paramString := strings.Join(sortedParams, "")
	h.Write([]byte(paramString))
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Service) makeWeatherLinkRequest(url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	// Add X-Api-Secret header
	req.Header.Set("X-Api-Secret", s.config.WeatherLinkAPISecret)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

func (s *Service) fetchSensorMetadata(ctx context.Context) error {
	url := fmt.Sprintf("https://api.weatherlink.com/v2/sensors?api-key=%s", s.config.WeatherLinkAPIKey)
	
	log.Println("Fetching sensor metadata...")
	body, err := s.makeWeatherLinkRequest(url)
	if err != nil {
		return fmt.Errorf("failed to fetch sensor metadata: %w", err)
	}

	var response SensorsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("failed to parse sensor metadata: %w", err)
	}

	// Check if metadata has changed
	hash := calculateHash(body)
	if s.lastMetadataHash["sensors"] == hash {
		log.Println("Sensor metadata unchanged, skipping publish")
		return nil
	}

	// Update sensor map
	for _, sensor := range response.Sensors {
		s.sensorMap[sensor.LSID] = sensor
		
		// Publish each sensor to metadata topic
		if err := s.publishToKafka(ctx, "weather.metadata.sensors", sensor, map[string]string{
			"lsid":         strconv.Itoa(sensor.LSID),
			"sensor_type":  strconv.Itoa(sensor.SensorType),
			"category":     sensor.Category,
			"station_id":   strconv.Itoa(sensor.StationID),
		}); err != nil {
			log.Printf("Failed to publish sensor metadata: %v", err)
		}
	}

	s.lastMetadataHash["sensors"] = hash
	log.Printf("Published metadata for %d sensors", len(response.Sensors))
	return nil
}

func (s *Service) fetchSensorCatalog(ctx context.Context) error {
	url := fmt.Sprintf("https://api.weatherlink.com/v2/sensor-catalog?api-key=%s", s.config.WeatherLinkAPIKey)
	
	log.Println("Fetching sensor catalog...")
	body, err := s.makeWeatherLinkRequest(url)
	if err != nil {
		return fmt.Errorf("failed to fetch sensor catalog: %w", err)
	}

	// Check if catalog has changed
	hash := calculateHash(body)
	if s.lastMetadataHash["catalog"] == hash {
		log.Println("Sensor catalog unchanged, skipping publish")
		return nil
	}

	var response SensorCatalogResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("failed to parse sensor catalog: %w", err)
	}

	// Publish catalog to metadata topic
	if err := s.publishToKafka(ctx, "weather.metadata.catalog", response, map[string]string{
		"entry_count": strconv.Itoa(len(response.SensorCatalog)),
	}); err != nil {
		return fmt.Errorf("failed to publish catalog: %w", err)
	}

	s.lastMetadataHash["catalog"] = hash
	log.Println("Published sensor catalog")
	return nil
}

func (s *Service) fetchStationInfo(ctx context.Context) error {
	url := fmt.Sprintf("https://api.weatherlink.com/v2/stations/%s?api-key=%s", 
		s.config.WeatherLinkStationID, s.config.WeatherLinkAPIKey)
	
	log.Println("Fetching station info...")
	body, err := s.makeWeatherLinkRequest(url)
	if err != nil {
		return fmt.Errorf("failed to fetch station info: %w", err)
	}

	// Check if station info has changed
	hash := calculateHash(body)
	if s.lastMetadataHash["station"] == hash {
		log.Println("Station info unchanged, skipping publish")
		return nil
	}

	var response StationResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("failed to parse station info: %w", err)
	}

	// Publish station info to metadata topic
	if err := s.publishToKafka(ctx, "weather.metadata.station", response, map[string]string{
		"station_id": s.config.WeatherLinkStationID,
	}); err != nil {
		return fmt.Errorf("failed to publish station info: %w", err)
	}

	s.lastMetadataHash["station"] = hash
	log.Println("Published station info")
	return nil
}

func (s *Service) rehydrateCache(ctx context.Context) error {
	if s.db == nil {
		log.Println("PostgreSQL not configured, skipping cache rehydration")
		return nil
	}

	log.Println("Rehydrating timestamp cache from PostgreSQL...")
	
	// Query for max timestamps per device from last 24 hours
	query := `
		SELECT device_id, MAX(timestamp) as last_timestamp
		FROM (
			SELECT device_id, timestamp FROM records_numeric WHERE timestamp > $1
			UNION ALL
			SELECT device_id, timestamp FROM records_text WHERE timestamp > $1
			UNION ALL
			SELECT device_id, timestamp FROM records_null WHERE timestamp > $1
		) combined
		GROUP BY device_id
	`
	
	// Calculate 24 hours ago in Unix timestamp
	oneDayAgo := time.Now().Add(-24 * time.Hour).Unix()
	
	rows, err := s.db.QueryContext(ctx, query, oneDayAgo)
	if err != nil {
		// If tables don't exist yet, that's okay - cache starts empty
		log.Printf("Warning: Could not rehydrate cache (tables may not exist yet): %v", err)
		return nil
	}
	defer rows.Close()

	s.cacheMutex.Lock()
	defer s.cacheMutex.Unlock()

	count := 0
	for rows.Next() {
		var deviceID int
		var lastTimestamp int64
		if err := rows.Scan(&deviceID, &lastTimestamp); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		s.timestampCache[deviceID] = lastTimestamp
		count++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	log.Printf("Rehydrated cache with %d device timestamps", count)
	return nil
}

func (s *Service) fetchCurrentConditions(ctx context.Context) error {
	url := fmt.Sprintf("https://api.weatherlink.com/v2/current/%s?api-key=%s", 
		s.config.WeatherLinkStationID, s.config.WeatherLinkAPIKey)
	
	log.Println("Fetching current conditions...")
	body, err := s.makeWeatherLinkRequest(url)
	if err != nil {
		return fmt.Errorf("failed to fetch current conditions: %w", err)
	}

	var response CurrentConditionsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("failed to parse current conditions: %w", err)
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
			s.cacheMutex.RLock()
			lastTimestamp := s.timestampCache[sensor.LSID]
			s.cacheMutex.RUnlock()

			if timestamp > 0 && timestamp == lastTimestamp {
				messagesSkipped++
				continue
			}

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

			if err := s.publishToKafka(ctx, topic, dataPoint, headers); err != nil {
				log.Printf("Failed to publish data point: %v", err)
			} else {
				messagesPublished++
				
				// Update timestamp cache
				if timestamp > 0 {
					s.cacheMutex.Lock()
					s.timestampCache[sensor.LSID] = timestamp
					s.cacheMutex.Unlock()
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

func (s *Service) publishToKafka(ctx context.Context, topic string, data interface{}, headers map[string]string) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// Convert headers to kafka.Header format
	kafkaHeaders := make([]kafka.Header, 0, len(headers))
	for key, value := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   key,
			Value: []byte(value),
		})
	}

	msg := kafka.Message{
		Topic:   topic,
		Value:   jsonData,
		Headers: kafkaHeaders,
		Time:    time.Now(),
	}

	return s.kafkaWriter.WriteMessages(ctx, msg)
}

func calculateHash(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func (s *Service) Start(ctx context.Context) error {
	// Connect to PostgreSQL if configured
	if s.config.PostgresDSN != "" {
		log.Println("Connecting to PostgreSQL...")
		db, err := sql.Open("postgres", s.config.PostgresDSN)
		if err != nil {
			log.Printf("Warning: Failed to connect to PostgreSQL: %v", err)
		} else {
			// Test connection
			if err := db.PingContext(ctx); err != nil {
				log.Printf("Warning: PostgreSQL connection test failed: %v", err)
				db.Close()
			} else {
				s.db = db
				log.Println("Connected to PostgreSQL")
				
				// Rehydrate cache from database
				if err := s.rehydrateCache(ctx); err != nil {
					log.Printf("Warning: Failed to rehydrate cache: %v", err)
				}
			}
		}
	}

	// Fetch metadata on startup
	if err := s.fetchSensorMetadata(ctx); err != nil {
		log.Printf("Warning: Failed to fetch sensor metadata: %v", err)
	}
	
	if err := s.fetchSensorCatalog(ctx); err != nil {
		log.Printf("Warning: Failed to fetch sensor catalog: %v", err)
	}
	
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

func (s *Service) Close() error {
	if s.db != nil {
		s.db.Close()
	}
	return s.kafkaWriter.Close()
}

func main() {
	log.Println("Starting ROACH Weather Service...")

	config := loadConfig()

	// Validate configuration
	if config.WeatherLinkAPIKey == "" {
		log.Fatal("WEATHERLINK_API_KEY is required")
	}
	if config.WeatherLinkAPISecret == "" {
		log.Fatal("WEATHERLINK_API_SECRET is required")
	}
	if config.WeatherLinkStationID == "" {
		log.Fatal("WEATHERLINK_STATION_ID is required")
	}
	if config.KafkaBroker == "" {
		log.Fatal("KAFKA_BROKER is required")
	}

	log.Printf("Configuration loaded:")
	log.Printf("  - Station ID: %s", config.WeatherLinkStationID)
	log.Printf("  - Kafka Broker: %s", config.KafkaBroker)
	log.Printf("  - Fetch Interval: %s", config.FetchInterval)

	service := NewService(config)
	defer service.Close()

	ctx := context.Background()
	if err := service.Start(ctx); err != nil {
		log.Fatalf("Service error: %v", err)
	}
}
