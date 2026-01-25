package models

import (
	"encoding/json"
	"time"
)

// Config holds the application configuration
type Config struct {
	WeatherLinkAPIKey    string
	WeatherLinkAPISecret string
	WeatherLinkStationID string
	KafkaBroker          string
	PostgresDSN          string
	FetchInterval        time.Duration
	LogLevel             string
}

// CurrentConditionsResponse represents the response from the current conditions API
type CurrentConditionsResponse struct {
	StationID     int      `json:"station_id"`
	StationIDUUID string   `json:"station_id_uuid"`
	Sensors       []Sensor `json:"sensors"`
	GeneratedAt   int64    `json:"generated_at"`
}

// Sensor represents a sensor in the current conditions response
type Sensor struct {
	LSID              int               `json:"lsid"`
	SensorType        int               `json:"sensor_type"`
	DataStructureType int               `json:"data_structure_type"`
	Data              []json.RawMessage `json:"data"`
}

// SensorsResponse represents the response from the sensors API
type SensorsResponse struct {
	Sensors     []SensorMetadata `json:"sensors"`
	GeneratedAt int64            `json:"generated_at"`
}

// SensorMetadata contains metadata about a sensor
type SensorMetadata struct {
	LSID              int     `json:"lsid"`
	SensorType        int     `json:"sensor_type"`
	Category          string  `json:"category"`
	Manufacturer      string  `json:"manufacturer"`
	ProductName       string  `json:"product_name"`
	ProductNumber     string  `json:"product_number"`
	RainCollectorType int     `json:"rain_collector_type"`
	Active            bool    `json:"active"`
	TxID              *int    `json:"tx_id"`
	PortNumber        int     `json:"port_number"`
	ParentDeviceType  string  `json:"parent_device_type"`
	ParentDeviceName  string  `json:"parent_device_name"`
	ParentDeviceID    int64   `json:"parent_device_id"`
	ParentDeviceIDHex string  `json:"parent_device_id_hex"`
	StationID         int     `json:"station_id"`
	StationIDUUID     string  `json:"station_id_uuid"`
	StationName       string  `json:"station_name"`
	Latitude          float64 `json:"latitude"`
	Longitude         float64 `json:"longitude"`
	Elevation         float64 `json:"elevation"`
}

// StationResponse represents the response from the station API
type StationResponse struct {
	Stations    []StationInfo `json:"stations"`
	GeneratedAt int64         `json:"generated_at"`
}

// StationInfo contains information about a weather station
type StationInfo struct {
	StationID     int    `json:"station_id"`
	StationIDUUID string `json:"station_id_uuid"`
	StationName   string `json:"station_name"`
}

// SensorCatalogResponse represents the response from the sensor catalog API
type SensorCatalogResponse struct {
	SensorCatalog []CatalogEntry `json:"sensor_types"`
	GeneratedAt   int64          `json:"generated_at"`
}

// CatalogEntry represents a sensor type in the catalog
type CatalogEntry struct {
	SensorType     int             `json:"sensor_type"`
	Manufacturer   string          `json:"manufacturer"`
	ProductName    string          `json:"product_name"`
	ProductNumber  string          `json:"product_number"`
	Category       string          `json:"category"`
	DataStructures []DataStructure `json:"data_structures"`
}

// DataStructure represents a data structure definition
type DataStructure struct {
	DataStructureType string                     `json:"data_structure_type"`
	Description       string                     `json:"description"`
	DataStructure     map[string]FieldDefinition `json:"data_structure"`
}

// FieldDefinition represents a field definition in a data structure
type FieldDefinition struct {
	Type  string `json:"type"`
	Units string `json:"units"`
}
