package ha

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"homeassistant-command/models"
)

// Client manages a persistent WebSocket connection to Home Assistant
// and executes service calls on demand.
type Client struct {
	cfg    models.Config
	dialer *websocket.Dialer

	mu   sync.Mutex
	conn *websocket.Conn
	msgID atomic.Int64
}

// NewClient creates a new Home Assistant WebSocket client.
func NewClient(cfg models.Config) *Client {
	return &Client{
		cfg:    cfg,
		dialer: websocket.DefaultDialer,
	}
}

// Connect establishes and authenticates a WebSocket connection to Home Assistant.
// If a connection already exists it is closed first.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}

	conn, _, err := c.dialer.Dial(c.cfg.HAWSURL, nil)
	if err != nil {
		return fmt.Errorf("ws dial failed: %w", err)
	}

	if err := expectAuthRequired(conn); err != nil {
		conn.Close()
		return err
	}
	if err := sendJSON(conn, map[string]string{
		"type":         "auth",
		"access_token": c.cfg.HAToken,
	}); err != nil {
		conn.Close()
		return err
	}
	if err := expectAuthOK(conn); err != nil {
		conn.Close()
		return err
	}

	c.conn = conn
	c.msgID.Store(0)
	log.Println("Connected and authenticated to Home Assistant WebSocket")
	return nil
}

// ConnectWithRetry connects to Home Assistant, retrying with the configured backoff.
func (c *Client) ConnectWithRetry(ctx context.Context) error {
	attempt := 0
	for {
		err := c.Connect(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		backoff := c.cfg.WSReconnectBackoff
		wait := backoff[len(backoff)-1]
		if attempt < len(backoff) {
			wait = backoff[attempt]
		}
		attempt++

		log.Printf("WebSocket connection failed: %v — retrying in %s", err, wait)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// CallService sends a call_service command over the WebSocket and waits for
// the result. Returns an error if the command fails or the connection is broken.
func (c *Client) CallService(ctx context.Context, cmd models.Command) error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()

	if conn == nil {
		return fmt.Errorf("not connected to Home Assistant")
	}

	id := int(c.msgID.Add(1))

	msg := map[string]interface{}{
		"id":     id,
		"type":   "call_service",
		"domain": cmd.Domain,
		"service": cmd.Service,
		"target": map[string]interface{}{
			"entity_id": cmd.EntityID,
		},
	}
	if len(cmd.Data) > 0 {
		msg["service_data"] = cmd.Data
	}

	c.mu.Lock()
	err := sendJSON(conn, msg)
	c.mu.Unlock()
	if err != nil {
		return fmt.Errorf("failed to send call_service: %w", err)
	}

	// Wait for the result message matching our ID
	result, err := c.readResult(conn, id, 30*time.Second)
	if err != nil {
		return fmt.Errorf("failed to read call_service result: %w", err)
	}
	if !result.Success {
		return fmt.Errorf("call_service failed (id=%d): %s", id, result.ErrorMsg)
	}

	return nil
}

// Ping sends a ping message to Home Assistant and waits for the pong.
func (c *Client) Ping(ctx context.Context) error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()

	if conn == nil {
		return fmt.Errorf("not connected")
	}

	id := int(c.msgID.Add(1))

	c.mu.Lock()
	err := sendJSON(conn, map[string]interface{}{
		"id":   id,
		"type": "ping",
	})
	c.mu.Unlock()
	if err != nil {
		return fmt.Errorf("ping send failed: %w", err)
	}

	// Read until we get the matching pong
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn.SetReadDeadline(deadline)
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("ping read failed: %w", err)
		}
		var msg struct {
			ID   int    `json:"id"`
			Type string `json:"type"`
		}
		if err := json.Unmarshal(payload, &msg); err != nil {
			continue
		}
		if msg.Type == "pong" && msg.ID == id {
			conn.SetReadDeadline(time.Time{}) // clear deadline
			return nil
		}
	}
	return fmt.Errorf("ping timeout")
}

// Close cleanly shuts down the WebSocket connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil
	}
	_ = c.conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown"),
		time.Now().Add(2*time.Second),
	)
	err := c.conn.Close()
	c.conn = nil
	return err
}

// IsConnected reports whether a WebSocket connection currently exists.
func (c *Client) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil
}

// ClearConnection marks the connection as closed so the next call triggers a reconnect.
func (c *Client) ClearConnection() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

// --- internal helpers ---

type resultMessage struct {
	ID       int    `json:"id"`
	Type     string `json:"type"`
	Success  bool   `json:"success"`
	ErrorMsg string `json:"-"`
}

type resultRaw struct {
	ID      int    `json:"id"`
	Type    string `json:"type"`
	Success bool   `json:"success"`
	Error   *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func (c *Client) readResult(conn *websocket.Conn, expectedID int, timeout time.Duration) (*resultMessage, error) {
	deadline := time.Now().Add(timeout)
	conn.SetReadDeadline(deadline)
	defer conn.SetReadDeadline(time.Time{})

	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return nil, err
		}

		var raw resultRaw
		if err := json.Unmarshal(payload, &raw); err != nil {
			continue
		}
		if raw.Type != "result" || raw.ID != expectedID {
			// Skip messages that don't match (e.g. events from other subscriptions)
			continue
		}

		result := &resultMessage{
			ID:      raw.ID,
			Type:    raw.Type,
			Success: raw.Success,
		}
		if raw.Error != nil {
			result.ErrorMsg = fmt.Sprintf("%s: %s", raw.Error.Code, raw.Error.Message)
		}
		return result, nil
	}
}

type authMessage struct {
	Type    string `json:"type"`
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
