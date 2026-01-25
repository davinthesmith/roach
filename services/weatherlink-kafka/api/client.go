package api

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client handles communication with the WeatherLink API
type Client struct {
	apiKey     string
	apiSecret  string
	stationID  string
	httpClient *http.Client
}

// NewClient creates a new WeatherLink API client
func NewClient(apiKey, apiSecret, stationID string) *Client {
	return &Client{
		apiKey:    apiKey,
		apiSecret: apiSecret,
		stationID: stationID,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// makeRequest makes a request to the WeatherLink API
func (c *Client) makeRequest(url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	// Add X-Api-Secret header
	req.Header.Set("X-Api-Secret", c.apiSecret)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}
