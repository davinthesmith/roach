package api

import (
	"encoding/json"
	"fmt"
	"log"

	"weather/models"
)

// FetchCurrentConditions fetches current conditions from the WeatherLink API
func (c *Client) FetchCurrentConditions() (*models.CurrentConditionsResponse, error) {
	url := fmt.Sprintf("https://api.weatherlink.com/v2/current/%s?api-key=%s",
		c.stationID, c.apiKey)

	log.Println("Fetching current conditions...")
	body, err := c.makeRequest(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch current conditions: %w", err)
	}

	var response models.CurrentConditionsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse current conditions: %w", err)
	}

	return &response, nil
}

// FetchSensorMetadata fetches sensor metadata from the WeatherLink API
func (c *Client) FetchSensorMetadata() (*models.SensorsResponse, error) {
	url := fmt.Sprintf("https://api.weatherlink.com/v2/sensors?api-key=%s", c.apiKey)

	log.Println("Fetching sensor metadata...")
	body, err := c.makeRequest(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch sensor metadata: %w", err)
	}

	var response models.SensorsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse sensor metadata: %w", err)
	}

	return &response, nil
}

// FetchSensorCatalog fetches the sensor catalog from the WeatherLink API
func (c *Client) FetchSensorCatalog() (*models.SensorCatalogResponse, error) {
	url := fmt.Sprintf("https://api.weatherlink.com/v2/sensor-catalog?api-key=%s", c.apiKey)

	log.Println("Fetching sensor catalog...")
	body, err := c.makeRequest(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch sensor catalog: %w", err)
	}

	var response models.SensorCatalogResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse sensor catalog: %w", err)
	}

	return &response, nil
}

// FetchStationInfo fetches station information from the WeatherLink API
func (c *Client) FetchStationInfo() (*models.StationResponse, error) {
	url := fmt.Sprintf("https://api.weatherlink.com/v2/stations/%s?api-key=%s",
		c.stationID, c.apiKey)

	log.Println("Fetching station info...")
	body, err := c.makeRequest(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch station info: %w", err)
	}

	var response models.StationResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse station info: %w", err)
	}

	return &response, nil
}
