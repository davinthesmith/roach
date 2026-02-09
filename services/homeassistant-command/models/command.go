package models

// Command represents a Home Assistant service call received from Kafka.
type Command struct {
	Domain   string                 `json:"domain"`
	Service  string                 `json:"service"`
	EntityID string                 `json:"entity_id"`
	Data     map[string]interface{} `json:"data"`
}

// Validate checks that required fields are present.
func (c *Command) Validate() error {
	if c.Domain == "" {
		return &ValidationError{Field: "domain", Message: "domain is required"}
	}
	if c.Service == "" {
		return &ValidationError{Field: "service", Message: "service is required"}
	}
	if c.EntityID == "" {
		return &ValidationError{Field: "entity_id", Message: "entity_id is required"}
	}
	return nil
}

// ValidationError represents a command validation failure.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}
