package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"homeassistant-command/ha"
	"homeassistant-command/models"
)

// Service orchestrates consuming Kafka commands and forwarding them to Home Assistant.
type Service struct {
	cfg      models.Config
	haClient *ha.Client
	reader   *kafkago.Reader
}

// New creates a new command service.
func New(cfg models.Config, haClient *ha.Client, reader *kafkago.Reader) *Service {
	return &Service{
		cfg:      cfg,
		haClient: haClient,
		reader:   reader,
	}
}

// Start runs the main consume loop. It blocks until the context is cancelled.
func (s *Service) Start(ctx context.Context) error {
	// Establish the initial WebSocket connection
	if err := s.haClient.ConnectWithRetry(ctx); err != nil {
		return fmt.Errorf("initial HA connection failed: %w", err)
	}

	// Start keepalive pinger in background
	go s.keepAlive(ctx)

	log.Printf("Consuming commands from topic %q (group: %s)", s.cfg.KafkaTopic, s.cfg.KafkaConsumerGroup)

	for {
		msg, err := s.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("Kafka read error: %v", err)
			continue
		}

		s.processMessage(ctx, msg)

		// Commit the offset after processing (success or permanent failure)
		if err := s.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Kafka commit error: %v", err)
		}
	}
}

func (s *Service) processMessage(ctx context.Context, msg kafkago.Message) {
	var cmd models.Command
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		log.Printf("Failed to parse command (offset=%d): %v — body: %s", msg.Offset, err, string(msg.Value))
		return
	}

	if err := cmd.Validate(); err != nil {
		log.Printf("Invalid command (offset=%d): %v — body: %s", msg.Offset, err, string(msg.Value))
		return
	}

	if s.cfg.LogLevel == "debug" {
		log.Printf("DEBUG: command received: domain=%s service=%s entity=%s data=%v",
			cmd.Domain, cmd.Service, cmd.EntityID, cmd.Data)
	}

	log.Printf("Executing: %s.%s -> %s", cmd.Domain, cmd.Service, cmd.EntityID)

	if err := s.executeWithReconnect(ctx, cmd); err != nil {
		log.Printf("Command failed (offset=%d): %v", msg.Offset, err)
		return
	}

	log.Printf("Success: %s.%s -> %s", cmd.Domain, cmd.Service, cmd.EntityID)
}

// executeWithReconnect attempts to call the HA service, reconnecting once if
// the WebSocket connection is broken.
func (s *Service) executeWithReconnect(ctx context.Context, cmd models.Command) error {
	err := s.haClient.CallService(ctx, cmd)
	if err == nil {
		return nil
	}

	log.Printf("call_service failed: %v — reconnecting", err)
	s.haClient.ClearConnection()

	if err := s.haClient.ConnectWithRetry(ctx); err != nil {
		return fmt.Errorf("reconnect failed: %w", err)
	}

	// Retry the command on the fresh connection
	return s.haClient.CallService(ctx, cmd)
}

// keepAlive sends periodic pings to Home Assistant to keep the WebSocket
// connection alive during idle periods. If a ping fails, it automatically
// reconnects so the service stays healthy between commands.
func (s *Service) keepAlive(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.haClient.IsConnected() {
				continue
			}
			if err := s.haClient.Ping(ctx); err != nil {
				log.Printf("Keepalive ping failed: %v — reconnecting", err)
				s.haClient.ClearConnection()
				if err := s.haClient.ConnectWithRetry(ctx); err != nil {
					log.Printf("Keepalive reconnect failed: %v", err)
				}
			}
		}
	}
}

// Close releases resources held by the service.
func (s *Service) Close() error {
	var firstErr error
	if err := s.reader.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("kafka reader close: %w", err)
	}
	if err := s.haClient.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("ha client close: %w", err)
	}
	return firstErr
}
