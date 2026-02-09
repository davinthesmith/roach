package ha

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"homeassistant-kafka/models"
)

type EventHandler func(event models.HAEvent, raw json.RawMessage)

type Client struct {
	cfg        models.Config
	httpClient *http.Client
	wsDialer   *websocket.Dialer
}

func NewClient(cfg models.Config) *Client {
	return &Client{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		wsDialer: websocket.DefaultDialer,
	}
}

// SubscribeStateChanges connects to the Home Assistant websocket and streams state_changed events.
func (c *Client) SubscribeStateChanges(ctx context.Context, handler EventHandler) error {
	attempt := 0
	for {
		err := c.connectAndStream(ctx, handler)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		backoff := c.cfg.WSReconnectBackoff
		wait := backoff[len(backoff)-1]
		if len(backoff) > 0 && attempt < len(backoff) {
			wait = backoff[attempt]
		}
		attempt++

		log.Printf("WebSocket disconnected: %v", err)
		log.Printf("Reconnecting in %s", wait)

		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *Client) connectAndStream(ctx context.Context, handler EventHandler) error {
	conn, _, err := c.wsDialer.Dial(c.cfg.HAWSURL, nil)
	if err != nil {
		return fmt.Errorf("ws dial failed: %w", err)
	}
	defer conn.Close()

	closeOnCancel(ctx, conn)

	if err := expectAuthRequired(conn); err != nil {
		return err
	}
	if err := sendJSON(conn, map[string]string{
		"type":         "auth",
		"access_token": c.cfg.HAToken,
	}); err != nil {
		return err
	}
	if err := expectAuthOK(conn); err != nil {
		return err
	}

	subscribeID := 1
	if err := sendJSON(conn, map[string]interface{}{
		"id":         subscribeID,
		"type":       "subscribe_events",
		"event_type": "state_changed",
	}); err != nil {
		return err
	}
	if err := expectSubscribeOK(conn, subscribeID); err != nil {
		return err
	}

	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("ws read failed: %w", err)
		}

		var envelope models.HAEventEnvelope
		if err := json.Unmarshal(payload, &envelope); err != nil {
			log.Printf("Failed to parse message: %v", err)
			continue
		}
		if envelope.Type != "event" || len(envelope.Event) == 0 {
			continue
		}
		var event models.HAEvent
		if err := json.Unmarshal(envelope.Event, &event); err != nil {
			log.Printf("Failed to parse event: %v", err)
			continue
		}
		handler(event, envelope.Event)
	}
}

func closeOnCancel(ctx context.Context, conn *websocket.Conn) {
	go func() {
		<-ctx.Done()
		_ = conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown"),
			time.Now().Add(2*time.Second),
		)
		_ = conn.Close()
	}()
}

type authMessage struct {
	Type    string `json:"type"`
	Message string `json:"message,omitempty"`
}

type resultMessage struct {
	ID      int    `json:"id"`
	Type    string `json:"type"`
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

func expectAuthRequired(conn *websocket.Conn) error {
	var msg authMessage
	if err := readJSON(conn, &msg); err != nil {
		return err
	}
	if msg.Type != "auth_required" {
		return fmt.Errorf("expected auth_required, got %s", msg.Type)
	}
	return nil
}

func expectAuthOK(conn *websocket.Conn) error {
	var msg authMessage
	if err := readJSON(conn, &msg); err != nil {
		return err
	}
	if msg.Type != "auth_ok" {
		if msg.Message != "" {
			return fmt.Errorf("auth failed: %s", msg.Message)
		}
		return fmt.Errorf("auth failed: %s", msg.Type)
	}
	return nil
}

func expectSubscribeOK(conn *websocket.Conn, id int) error {
	for {
		var msg resultMessage
		if err := readJSON(conn, &msg); err != nil {
			return err
		}
		if msg.Type != "result" || msg.ID != id {
			continue
		}
		if !msg.Success {
			return fmt.Errorf("subscribe failed: %s", msg.Message)
		}
		return nil
	}
}

func readJSON(conn *websocket.Conn, v interface{}) error {
	_, payload, err := conn.ReadMessage()
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, v)
}

func sendJSON(conn *websocket.Conn, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, data)
}

// DiscoverEntities queries the HA entity registry via WebSocket and returns
// entity_ids that belong to the given platform (e.g. "ecobee").
func (c *Client) DiscoverEntities(ctx context.Context, platform string) ([]string, error) {
	conn, _, err := c.wsDialer.Dial(c.cfg.HAWSURL, nil)
	if err != nil {
		return nil, fmt.Errorf("ws dial failed: %w", err)
	}
	defer conn.Close()

	// Authenticate
	if err := expectAuthRequired(conn); err != nil {
		return nil, fmt.Errorf("entity discovery auth handshake: %w", err)
	}
	if err := sendJSON(conn, map[string]string{
		"type":         "auth",
		"access_token": c.cfg.HAToken,
	}); err != nil {
		return nil, fmt.Errorf("entity discovery send auth: %w", err)
	}
	if err := expectAuthOK(conn); err != nil {
		return nil, fmt.Errorf("entity discovery auth: %w", err)
	}

	// Request entity registry
	if err := sendJSON(conn, map[string]interface{}{
		"id":   1,
		"type": "config/entity_registry/list",
	}); err != nil {
		return nil, fmt.Errorf("entity discovery request: %w", err)
	}

	// Read response
	_, payload, err := conn.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("entity discovery read: %w", err)
	}

	var resp struct {
		ID      int                          `json:"id"`
		Type    string                       `json:"type"`
		Success bool                         `json:"success"`
		Result  []models.HAEntityRegistryEntry `json:"result"`
	}
	if err := json.Unmarshal(payload, &resp); err != nil {
		return nil, fmt.Errorf("entity discovery parse: %w", err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("entity discovery failed: response not successful")
	}

	// Filter by platform
	var entityIDs []string
	for _, entry := range resp.Result {
		if strings.EqualFold(entry.Platform, platform) {
			entityIDs = append(entityIDs, entry.EntityID)
		}
	}
	return entityIDs, nil
}

// FetchStates uses the REST API to get the full entity state list.
func (c *Client) FetchStates(ctx context.Context) ([]models.HAState, error) {
	url := strings.TrimRight(c.cfg.HAURL, "/") + "/api/states"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.HAToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ha rest error: %s", resp.Status)
	}

	var states []models.HAState
	if err := json.NewDecoder(resp.Body).Decode(&states); err != nil {
		return nil, err
	}
	return states, nil
}
