package models

import "encoding/json"

// HAEventEnvelope is the websocket wrapper for Home Assistant events.
type HAEventEnvelope struct {
	ID      int             `json:"id,omitempty"`
	Type    string          `json:"type"`
	Success bool            `json:"success,omitempty"`
	Event   json.RawMessage `json:"event,omitempty"`
	Message string          `json:"message,omitempty"`
}

// HAEvent represents the Home Assistant event payload.
type HAEvent struct {
	EventType string      `json:"event_type"`
	Data      HAEventData `json:"data"`
	Origin    string      `json:"origin"`
	TimeFired string      `json:"time_fired"`
	Context   HAContext   `json:"context"`
}

type HAEventData struct {
	EntityID string   `json:"entity_id"`
	OldState *HAState `json:"old_state"`
	NewState *HAState `json:"new_state"`
}

type HAState struct {
	EntityID    string                 `json:"entity_id"`
	State       string                 `json:"state"`
	Attributes  map[string]interface{} `json:"attributes"`
	LastChanged string                 `json:"last_changed"`
	LastUpdated string                 `json:"last_updated"`
	Context     HAContext              `json:"context"`
}

type HAContext struct {
	ID       string `json:"id"`
	ParentID string `json:"parent_id"`
	UserID   string `json:"user_id"`
}

// HAEntityRegistryEntry represents a single entity from the HA entity registry.
type HAEntityRegistryEntry struct {
	EntityID string `json:"entity_id"`
	Platform string `json:"platform"`
}
