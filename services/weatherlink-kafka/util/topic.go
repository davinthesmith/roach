package util

import (
	"log"
	"strings"
)

// GetTopicForCategory determines the Kafka topic based on sensor category.
func GetTopicForCategory(category string) string {
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

// KeysFromSet returns the keys from a map[int]struct{}.
func KeysFromSet(m map[int]struct{}) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
