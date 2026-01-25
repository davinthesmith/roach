package models

// Config holds the application configuration
type Config struct {
	KafkaBroker string
	PostgresDSN string
	LogLevel    string
	BatchSize   int
}

// Device represents a sensor device
type Device struct {
	ID                int
	LSID              int
	SensorType        int
	DataStructureType *int
	Category          string
	Manufacturer      string
	ProductName       string
}

// Tag represents a data field/property for a device
type Tag struct {
	ID          int
	DeviceID    int
	TagName     string
	DataType    string
	Unit        *string
	Description *string
}

// FieldMetadata contains metadata about a specific field from the catalog
type FieldMetadata struct {
	FieldType         string
	Units             string
	Description       string
	DataStructureType string
	SensorType        int
	RawMetadata       map[string]interface{} // Store complete field definition from catalog
}
