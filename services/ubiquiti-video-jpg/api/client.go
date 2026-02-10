package api

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"ubiquiti-video-jpg/models"
)

// Client communicates with a local UniFi Protect NVR via its integration API.
type Client struct {
	apiKey     string
	host       string // NVR URL (e.g. "https://192.168.1.1")
	httpClient *http.Client
	logLevel   string
}

// NewClient creates a new UniFi API client that connects directly to a local NVR.
// TLS verification is disabled because NVRs use self-signed certificates.
func NewClient(apiKey, host, logLevel string) *Client {
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	return &Client{
		apiKey:     apiKey,
		host:       strings.TrimRight(host, "/"),
		httpClient: httpClient,
		logLevel:   logLevel,
	}
}

const basePath = "/proxy/protect/integration/v1"

// newRequest creates an authenticated request to the UniFi API.
func (c *Client) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	url := c.host + basePath + path
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-API-Key", c.apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.logLevel == "debug" {
		log.Printf("API request: %s %s", method, url)
	}
	return req, nil
}

// logResponseError logs detailed error information for failed API responses.
func (c *Client) logResponseError(resp *http.Response, body []byte) {
	log.Printf("API error response: status=%d", resp.StatusCode)
	log.Printf("  URL: %s", resp.Request.URL.String())
	log.Printf("  Response body: %s", string(body))
	for _, h := range []string{"Content-Type", "X-Request-Id", "Www-Authenticate", "X-Error-Code"} {
		if v := resp.Header.Get(h); v != "" {
			log.Printf("  Response header %s: %s", h, v)
		}
	}
}

// FetchCameras retrieves camera metadata for name resolution.
// Returns a map of camera ID → CameraInfo.
func (c *Client) FetchCameras(ctx context.Context) (map[string]models.CameraInfo, error) {
	req, err := c.newRequest(ctx, http.MethodGet, "/cameras", nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch cameras: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		c.logResponseError(resp, body)
		return nil, fmt.Errorf("cameras API returned %d: %s", resp.StatusCode, string(body))
	}

	var cameras []struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		State string `json:"state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cameras); err != nil {
		return nil, fmt.Errorf("decode cameras: %w", err)
	}

	result := make(map[string]models.CameraInfo, len(cameras))
	for _, cam := range cameras {
		result[cam.ID] = models.CameraInfo{ID: cam.ID, Name: cam.Name, State: cam.State}
	}
	return result, nil
}

// CreateRTSPSStream requests a new RTSPS stream URL for a given camera.
// POST /cameras/{id}/rtsps-stream with body {"qualities":["high"]}
// Returns the RTSPS URL string.
func (c *Client) CreateRTSPSStream(ctx context.Context, cameraID string) (string, error) {
	body := bytes.NewBufferString(`{"qualities":["high"]}`)
	path := fmt.Sprintf("/cameras/%s/rtsps-stream", cameraID)
	req, err := c.newRequest(ctx, http.MethodPost, path, body)
	if err != nil {
		return "", fmt.Errorf("create rtsps-stream request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("create rtsps-stream: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read rtsps-stream response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		c.logResponseError(resp, respBody)
		return "", fmt.Errorf("rtsps-stream API returned %d: %s", resp.StatusCode, string(respBody))
	}

	if c.logLevel == "debug" {
		log.Printf("RTSPS stream response for camera %s: %s", cameraID, string(respBody))
	}

	// Try parsing as {"high": "rtsps://..."} (quality -> URL map)
	var qualityMap map[string]string
	if err := json.Unmarshal(respBody, &qualityMap); err == nil {
		if url, ok := qualityMap["high"]; ok && url != "" {
			return url, nil
		}
		// Take any URL from the map
		for _, url := range qualityMap {
			if url != "" {
				return url, nil
			}
		}
	}

	// Try parsing as [{"quality": "high", "url": "rtsps://..."}]
	var streamArray []struct {
		Quality string `json:"quality"`
		URL     string `json:"url"`
	}
	if err := json.Unmarshal(respBody, &streamArray); err == nil && len(streamArray) > 0 {
		for _, s := range streamArray {
			if s.URL != "" {
				return s.URL, nil
			}
		}
	}

	// Try parsing as a plain string URL
	var plainURL string
	if err := json.Unmarshal(respBody, &plainURL); err == nil && plainURL != "" {
		return plainURL, nil
	}

	return "", fmt.Errorf("could not extract RTSPS URL from response: %s", string(respBody))
}
