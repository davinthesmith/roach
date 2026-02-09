package models

// ProtectEvent represents a UniFi Protect event from the event stream.
// Only fields used for classification and routing are modeled here;
// the raw JSON is forwarded to Kafka unchanged.
type ProtectEvent struct {
	ID               string   `json:"id"`
	ModelKey         string   `json:"modelKey"`
	Type             string   `json:"type"`
	Start            int64    `json:"start"`
	End              int64    `json:"end"`
	SmartDetectTypes []string `json:"smartDetectTypes"`
	Camera           string   `json:"camera"`
	Device           string   `json:"device"`
}

// DeviceID returns the device/camera identifier, checking both JSON field names.
func (e ProtectEvent) DeviceID() string {
	if e.Device != "" {
		return e.Device
	}
	return e.Camera
}

// EventCategory classifies an event for Kafka topic routing.
type EventCategory string

const (
	CategorySmart  EventCategory = "smart"
	CategoryAudio  EventCategory = "audio"
	CategoryMotion EventCategory = "motion"
)

// SmartDetectType constants for video AI detections.
const (
	DetectPerson  = "person"
	DetectVehicle = "vehicle"
	DetectAnimal  = "animal"
	DetectPackage = "package"
)

// SmartDetectType constants for audio AI detections.
const (
	DetectBabyCry  = "babyCry"
	DetectCOAlarm  = "coAlarm"
	DetectSmoke    = "smoke"
	DetectSpeak    = "speak"
)

// AudioDetectionTypes is the set of smart detect types classified as audio.
var AudioDetectionTypes = map[string]bool{
	DetectBabyCry: true,
	DetectCOAlarm: true,
	DetectSmoke:   true,
	DetectSpeak:   true,
}

// VideoDetectionTypes is the set of smart detect types classified as video (smart).
var VideoDetectionTypes = map[string]bool{
	DetectPerson:  true,
	DetectVehicle: true,
	DetectAnimal:  true,
	DetectPackage: true,
}

// CameraInfo holds basic camera metadata used for key derivation and headers.
type CameraInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
