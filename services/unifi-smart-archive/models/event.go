package models

// SmartEvent is the parsed value of a unifi.protect.smart message.
// Only fields needed for archiving are modeled; raw JSON is not stored.
type SmartEvent struct {
	ID               string   `json:"id"`
	Start            int64    `json:"start"` // milliseconds
	End              int64    `json:"end"`  // milliseconds, 0 if not yet ended
	SmartDetectTypes []string `json:"smartDetectTypes"`
}

// HasEnd returns true if the event has an end time (final update).
func (e SmartEvent) HasEnd() bool {
	return e.End > 0
}
