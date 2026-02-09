package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"homeassistant-kafka/ha"
	"homeassistant-kafka/kafka"
	"homeassistant-kafka/models"
)

type Service struct {
	cfg              models.Config
	haClient         *ha.Client
	producer         *kafka.Producer
	discoveredEntities map[string]bool
}

func New(cfg models.Config, haClient *ha.Client, producer *kafka.Producer) *Service {
	return &Service{
		cfg:      cfg,
		haClient: haClient,
		producer: producer,
	}
}

func (s *Service) Start(ctx context.Context) error {
	// Discover ecobee entities from HA entity registry (unless explicit filter is set)
	if len(s.cfg.EntityFilter) == 0 {
		entities, err := s.haClient.DiscoverEntities(ctx, "ecobee")
		if err != nil {
			log.Printf("WARNING: Entity discovery failed: %v (no entities will match)", err)
		} else if len(entities) == 0 {
			log.Printf("WARNING: No ecobee entities found in HA entity registry")
		} else {
			s.discoveredEntities = make(map[string]bool, len(entities))
			domainCounts := make(map[string]int)
			for _, eid := range entities {
				s.discoveredEntities[eid] = true
				domainCounts[entityDomain(eid)]++
			}
			log.Printf("Discovered %d ecobee entities from HA entity registry", len(entities))
			for domain, count := range domainCounts {
				log.Printf("  [%s] %d entities", domain, count)
			}
			for _, eid := range entities {
				log.Printf("  - %s", eid)
			}
			// Warn if no battery entities were discovered
			hasBattery := false
			for _, eid := range entities {
				if strings.Contains(strings.ToLower(eid), "battery") {
					hasBattery = true
					break
				}
			}
			if !hasBattery {
				log.Printf("WARNING: No battery entities discovered — battery entities may be disabled in Home Assistant or not exposed by the ecobee integration")
			}
		}
	}

	if s.cfg.PollEnabled {
		log.Printf("Running in poll mode (interval: %s)", s.cfg.PollInterval)
		s.pollLoop(ctx)
		return ctx.Err()
	}
	return s.haClient.SubscribeStateChanges(ctx, s.handleEvent)
}

func (s *Service) pollLoop(ctx context.Context) {
	log.Printf("Polling enabled: interval %s", s.cfg.PollInterval)
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	var previous map[string]models.HAState
	for {
		states, err := s.haClient.FetchStates(ctx)
		if err != nil {
			log.Printf("Polling error: %v", err)
		} else {
			current := make(map[string]models.HAState, len(states))
			for _, state := range states {
				current[state.EntityID] = state
			}
			s.emitStateChanges(previous, current)
			previous = current
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) emitStateChanges(previous, current map[string]models.HAState) {
	for entityID, state := range current {
		if !s.isEcobeeEntity(entityID) {
			continue
		}

		var prevState *models.HAState
		if previous != nil {
			if prev, ok := previous[entityID]; ok {
				prevCopy := prev
				prevState = &prevCopy
			}
		}

		if prevState != nil && !stateChanged(*prevState, state) {
			continue
		}

		stateCopy := state
		event := models.HAEvent{
			EventType: "state_changed",
			Data: models.HAEventData{
				EntityID: entityID,
				OldState: prevState,
				NewState: &stateCopy,
			},
			Origin:    "polling",
			TimeFired: time.Now().UTC().Format(time.RFC3339Nano),
		}

		payload, err := json.Marshal(event)
		if err != nil {
			log.Printf("Failed to marshal poll event: %v", err)
			continue
		}
		s.handleEvent(event, json.RawMessage(payload))
	}
}

func stateChanged(oldState, newState models.HAState) bool {
	if oldState.State != newState.State {
		return true
	}
	if oldState.LastChanged != newState.LastChanged {
		return true
	}
	if oldState.LastUpdated != newState.LastUpdated {
		return true
	}
	return false
}

func (s *Service) handleEvent(event models.HAEvent, raw json.RawMessage) {
	entityID := event.Data.EntityID
	if entityID == "" && event.Data.NewState != nil {
		entityID = event.Data.NewState.EntityID
	}
	if entityID == "" {
		return
	}
	if !s.isEcobeeEntity(entityID) {
		return
	}
	if event.Data.NewState == nil {
		return
	}

	topic := s.topicForEvent(entityID, event.Data.NewState)
	if topic == "" {
		return
	}

	if s.cfg.LogLevel == "debug" {
		log.Printf("DEBUG: routing entity=%s domain=%s -> topic=%s", entityID, entityDomain(entityID), topic)
	}

	timestamp := eventTimestamp(event)
	key := fmt.Sprintf("%s:%d", entityKey(entityID, topic), timestamp)
	headers := map[string]string{
		"schema_version": "1",
		"entity_id":      entityID,
		"domain":         entityDomain(entityID),
		"timestamp":      fmt.Sprintf("%d", timestamp),
		"source":         "homeassistant",
		"event_type":     event.EventType,
	}

	if err := s.producer.Publish(context.Background(), topic, key, raw, headers); err != nil {
		log.Printf("Publish error (topic=%s entity=%s): %v", topic, entityID, err)
	}
}

func (s *Service) isEcobeeEntity(entityID string) bool {
	// Explicit POLL_ENTITY_FILTER overrides everything
	if len(s.cfg.EntityFilter) > 0 {
		for _, filter := range s.cfg.EntityFilter {
			if entityID == filter {
				return true
			}
		}
		return false
	}
	// Use discovered entity set from HA entity registry
	if len(s.discoveredEntities) > 0 {
		return s.discoveredEntities[entityID]
	}
	// Fallback: substring match (original behavior)
	return strings.Contains(strings.ToLower(entityID), "ecobee")
}

func (s *Service) topicForEvent(entityID string, state *models.HAState) string {
	domain := entityDomain(entityID)
	attributes := state.Attributes

	switch domain {
	case "climate":
		return "homeassistant.ecobee.thermostat.climate"
	case "weather":
		return "homeassistant.ecobee.weather"
	case "sensor":
		if isBattery(attributes, entityID) {
			return "homeassistant.ecobee.sensor.battery"
		}
		if isTemperature(attributes) {
			return "homeassistant.ecobee.sensor.temperature"
		}
		if isHumidity(attributes) {
			return "homeassistant.ecobee.sensor.humidity"
		}
		return "homeassistant.ecobee.other"
	case "binary_sensor":
		if isPresence(attributes, entityID) {
			return "homeassistant.ecobee.sensor.presence"
		}
		return "homeassistant.ecobee.other"
	default:
		return "homeassistant.ecobee.other"
	}
}

func entityDomain(entityID string) string {
	parts := strings.SplitN(entityID, ".", 2)
	if len(parts) == 2 {
		return parts[0]
	}
	return ""
}

// entityKey derives a friendly key name from an entity ID by stripping the
// HA domain prefix and any sensor-type suffix that is redundant with the topic.
// For example: "sensor.jadyn_s_room_temperature" -> "jadyn_s_room"
func entityKey(entityID, topic string) string {
	// Strip domain prefix: "sensor.jadyn_s_room_temperature" -> "jadyn_s_room_temperature"
	name := entityID
	if i := strings.Index(entityID, "."); i >= 0 {
		name = entityID[i+1:]
	}
	// Strip sensor-type suffix based on topic
	suffixes := map[string]string{
		"homeassistant.ecobee.sensor.temperature": "_temperature",
		"homeassistant.ecobee.sensor.humidity":    "_humidity",
		"homeassistant.ecobee.sensor.presence":    "_occupancy",
		"homeassistant.ecobee.sensor.battery":     "_battery",
	}
	if suffix, ok := suffixes[topic]; ok {
		name = strings.TrimSuffix(name, suffix)
	}
	return name
}

func isTemperature(attrs map[string]interface{}) bool {
	if deviceClass(attrs) == "temperature" {
		return true
	}
	unit := unitOfMeasurement(attrs)
	return strings.Contains(unit, "°") || unit == "f" || unit == "c" || unit == "°f" || unit == "°c"
}

func isHumidity(attrs map[string]interface{}) bool {
	if deviceClass(attrs) == "humidity" {
		return true
	}
	return unitOfMeasurement(attrs) == "%"
}

func isBattery(attrs map[string]interface{}, entityID string) bool {
	if deviceClass(attrs) == "battery" {
		return true
	}
	return strings.Contains(strings.ToLower(entityID), "battery")
}

func isPresence(attrs map[string]interface{}, entityID string) bool {
	class := deviceClass(attrs)
	if class == "occupancy" || class == "presence" || class == "motion" {
		return true
	}
	lowerID := strings.ToLower(entityID)
	return strings.Contains(lowerID, "occupancy") || strings.Contains(lowerID, "presence")
}

func deviceClass(attrs map[string]interface{}) string {
	if attrs == nil {
		return ""
	}
	if value, ok := attrs["device_class"]; ok {
		if str, ok := value.(string); ok {
			return strings.ToLower(str)
		}
	}
	return ""
}

func unitOfMeasurement(attrs map[string]interface{}) string {
	if attrs == nil {
		return ""
	}
	if value, ok := attrs["unit_of_measurement"]; ok {
		if str, ok := value.(string); ok {
			return strings.ToLower(str)
		}
	}
	return ""
}

func eventTimestamp(event models.HAEvent) int64 {
	if event.TimeFired != "" {
		if ts, err := time.Parse(time.RFC3339Nano, event.TimeFired); err == nil {
			return ts.Unix()
		}
	}
	return time.Now().Unix()
}
