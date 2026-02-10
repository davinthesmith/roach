package api

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"unifi-kafka/models"

	"github.com/gorilla/websocket"
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
func (c *Client) newRequest(ctx context.Context, method, path string) (*http.Request, error) {
	url := c.host + basePath + path
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-API-Key", c.apiKey)
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
	// Log select response headers that may help debug
	for _, h := range []string{"Content-Type", "X-Request-Id", "Www-Authenticate", "X-Error-Code"} {
		if v := resp.Header.Get(h); v != "" {
			log.Printf("  Response header %s: %s", h, v)
		}
	}
}

// FetchCameras retrieves camera metadata for name resolution.
// Returns a map of camera ID → CameraInfo.
func (c *Client) FetchCameras(ctx context.Context) (map[string]models.CameraInfo, error) {
	req, err := c.newRequest(ctx, http.MethodGet, "/cameras")
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
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cameras); err != nil {
		return nil, fmt.Errorf("decode cameras: %w", err)
	}

	result := make(map[string]models.CameraInfo, len(cameras))
	for _, cam := range cameras {
		result[cam.ID] = models.CameraInfo{ID: cam.ID, Name: cam.Name}
	}
	return result, nil
}

// EventCallback is the function signature for processing events from the stream.
type EventCallback func(json.RawMessage) error

// eventWrapper represents the wrapper format from the UniFi API event stream.
// Format: {"type": "...", "item": {...}}
type eventWrapper struct {
	Type string          `json:"type"`
	Item json.RawMessage `json:"item"`
}

// SubscribeEvents connects to the NVR event stream via WebSocket and calls the
// callback for each event received. Blocks until the stream closes or the context
// is cancelled.
func (c *Client) SubscribeEvents(ctx context.Context, callback EventCallback) error {
	// Build the WebSocket URL: wss://host/proxy/protect/integration/v1/subscribe/events
	wsURL := strings.Replace(c.host, "https://", "wss://", 1)
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1)
	wsURL += basePath + "/subscribe/events"

	if c.logLevel == "debug" {
		log.Printf("WebSocket connecting to: %s", wsURL)
	}

	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	header := http.Header{}
	header.Set("X-API-Key", c.apiKey)

	conn, resp, err := dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("websocket dial failed (status %d): %s: %w", resp.StatusCode, string(body), err)
		}
		return fmt.Errorf("websocket dial failed: %w", err)
	}
	defer conn.Close()

	log.Printf("Connected to UniFi Protect event stream (WebSocket, status %d)", resp.StatusCode)

	// Close the WebSocket gracefully when context is cancelled
	go func() {
		<-ctx.Done()
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return fmt.Errorf("event stream closed by server")
			}
			return fmt.Errorf("websocket read error: %w", err)
		}

		c.processEventMessage(message, callback)
	}
}

// processEventMessage parses and dispatches a single event message.
func (c *Client) processEventMessage(data []byte, callback EventCallback) {
	line := strings.TrimSpace(string(data))
	if line == "" {
		return
	}

	// Parse as API wrapper: {"type": "...", "item": {...}}
	var wrapper eventWrapper
	if err := json.Unmarshal([]byte(line), &wrapper); err != nil {
		// Not valid JSON wrapper — pass raw data to callback
		if err := callback(json.RawMessage(data)); err != nil {
			log.Printf("Event handler error: %v", err)
		}
		return
	}

	// If wrapper has an item field, pass the unwrapped item
	if len(wrapper.Item) > 0 {
		if err := callback(wrapper.Item); err != nil {
			log.Printf("Event handler error: %v", err)
		}
	} else {
		// No item field — pass the full message
		if err := callback(json.RawMessage(data)); err != nil {
			log.Printf("Event handler error: %v", err)
		}
	}
}
