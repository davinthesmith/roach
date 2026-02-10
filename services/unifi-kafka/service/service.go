package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"unifi-kafka/api"
	"unifi-kafka/kafka"
	"unifi-kafka/models"
)

const (
	topicSmart  = "unifi.protect.smart"
	topicAudio  = "unifi.protect.audio"
	topicMotion = "unifi.protect.motion"
)

// Service orchestrates event ingestion from UniFi Protect to Kafka.
type Service struct {
	cfg       models.Config
	apiClient *api.Client
	producer  *kafka.Producer
	cameras   map[string]models.CameraInfo // camera ID -> info
}

// New creates a new Service instance.
func New(cfg models.Config, apiClient *api.Client, producer *kafka.Producer) *Service {
	return &Service{
		cfg:       cfg,
		apiClient: apiClient,
		producer:  producer,
		cameras:   make(map[string]models.CameraInfo),
	}
}

// Start discovers cameras and subscribes to the Protect event stream.
func (s *Service) Start(ctx context.Context) error {
	// Discover cameras for name resolution
	cameras, err := s.apiClient.FetchCameras(ctx)
	if err != nil {
		log.Printf("WARNING: Camera discovery failed: %v (camera names will use IDs)", err)
	} else {
		s.cameras = cameras
		log.Printf("Discovered %d cameras:", len(cameras))
		for _, cam := range cameras {
			log.Printf("  - %s (%s)", cam.Name, cam.ID)
		}
	}

	log.Println("Subscribing to UniFi Protect event stream...")
	return s.apiClient.SubscribeEvents(ctx, s.handleEvent)
}

// handleEvent processes a single event from the Protect event stream.
func (s *Service) handleEvent(raw json.RawMessage) error {
	var event models.ProtectEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		if s.cfg.LogLevel == "debug" {
			log.Printf("DEBUG: Skipping non-event payload: %s", string(raw))
		}
		return nil
	}

	// Only process event model keys
	if event.ModelKey != "event" {
		if s.cfg.LogLevel == "debug" {
			log.Printf("DEBUG: Ignoring modelKey=%s type=%s", event.ModelKey, event.Type)
		}
		return nil
	}

	// Classify and route the event
	category, detectionType := classifyEvent(event)
	if category == "" {
		if s.cfg.LogLevel == "debug" {
			log.Printf("DEBUG: Unclassified event id=%s smartDetectTypes=%v", event.ID, event.SmartDetectTypes)
		}
		return nil
	}

	topic := topicForCategory(category)
	deviceID := event.DeviceID()
	cameraName := s.cameraName(deviceID)
	timestamp := eventTimestamp(event)
	key := fmt.Sprintf("%s:%d", cameraName, timestamp)

	headers := map[string]string{
		"schema_version": "1",
		"camera_id":      deviceID,
		"camera_name":    cameraName,
		"event_type":     string(category),
		"detection_type": detectionType,
		"timestamp":      fmt.Sprintf("%d", timestamp),
		"source":         "unifi-protect",
	}

	if s.cfg.LogLevel == "debug" {
		log.Printf("DEBUG: Publishing event=%s category=%s detection=%s camera=%s -> topic=%s key=%s",
			event.ID, category, detectionType, cameraName, topic, key)
	} else {
		log.Printf("Event: %s/%s from %s", category, detectionType, cameraName)
	}

	if err := s.producer.PublishRaw(context.Background(), topic, key, raw, headers); err != nil {
		log.Printf("Publish error (topic=%s camera=%s): %v", topic, cameraName, err)
		return err
	}

	return nil
}

// classifyEvent determines the event category and specific detection type.
func classifyEvent(event models.ProtectEvent) (models.EventCategory, string) {
	// Check smartDetectTypes for AI detection events
	for _, dt := range event.SmartDetectTypes {
		if models.AudioDetectionTypes[dt] {
			return models.CategoryAudio, dt
		}
		if models.VideoDetectionTypes[dt] {
			return models.CategorySmart, dt
		}
	}

	// Check event type field for motion events
	eventType := strings.ToLower(event.Type)
	if eventType == "motion" || strings.Contains(eventType, "motion") {
		return models.CategoryMotion, "motion"
	}

	return "", ""
}

// topicForCategory maps an event category to a Kafka topic.
func topicForCategory(category models.EventCategory) string {
	switch category {
	case models.CategorySmart:
		return topicSmart
	case models.CategoryAudio:
		return topicAudio
	case models.CategoryMotion:
		return topicMotion
	default:
		return topicMotion
	}
}

// cameraName resolves a camera ID to a friendly name.
func (s *Service) cameraName(cameraID string) string {
	if cam, ok := s.cameras[cameraID]; ok {
		return sanitizeName(cam.Name)
	}
	return cameraID
}

// sanitizeName converts a camera name to a Kafka-key-friendly format.
// Example: "Courtyard" -> "courtyard", "Front Door" -> "front_door"
func sanitizeName(name string) string {
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	return name
}

// eventTimestamp extracts the best timestamp from an event (in seconds).
func eventTimestamp(event models.ProtectEvent) int64 {
	// Protect timestamps are in milliseconds
	if event.Start > 0 {
		return event.Start / 1000
	}
	if event.End > 0 {
		return event.End / 1000
	}
	return time.Now().Unix()
}
