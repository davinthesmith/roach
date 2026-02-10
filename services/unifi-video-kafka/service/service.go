package service

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"unifi-video-kafka/api"
	"unifi-video-kafka/kafka"
	"unifi-video-kafka/models"
	"unifi-video-kafka/stream"
)

const (
	topicPrefix       = "unifi.protect.video."
	offlinePollInterval = 1 * time.Hour
)

// Service orchestrates per-camera RTSPS streaming to Kafka.
type Service struct {
	cfg       models.Config
	apiClient *api.Client
	producer  *kafka.Producer
}

// New creates a new Service instance.
func New(cfg models.Config, apiClient *api.Client, producer *kafka.Producer) *Service {
	return &Service{
		cfg:       cfg,
		apiClient: apiClient,
		producer:  producer,
	}
}

// Start discovers cameras and launches one goroutine per camera.
// Blocks until context is cancelled.
func (s *Service) Start(ctx context.Context) error {
	cameras, err := s.apiClient.FetchCameras(ctx)
	if err != nil {
		return err
	}

	if len(cameras) == 0 {
		log.Println("No cameras found")
		return nil
	}

	var online, offline []models.CameraInfo
	for _, cam := range cameras {
		if cam.IsConnected() {
			online = append(online, cam)
		} else {
			offline = append(offline, cam)
		}
	}

	log.Printf("Discovered %d cameras (%d online, %d offline):", len(cameras), len(online), len(offline))
	for _, cam := range cameras {
		status := "ONLINE"
		if !cam.IsConnected() {
			status = "OFFLINE (" + cam.State + ")"
		}
		log.Printf("  - %s (%s) [%s]", cam.Name, cam.ID, status)
	}

	var wg sync.WaitGroup
	for _, cam := range cameras {
		wg.Add(1)
		go func(cam models.CameraInfo) {
			defer wg.Done()
			s.runCamera(ctx, cam)
		}(cam)
	}

	wg.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// runCamera manages a single camera's lifecycle.
// If the camera is offline, it polls hourly until connected.
// If the camera is online, it streams with backoff on failure.
// If streaming keeps failing immediately (never stable), it re-checks
// the camera state and switches to hourly polling if offline.
func (s *Service) runCamera(ctx context.Context, cam models.CameraInfo) {
	sanitized := sanitizeName(cam.Name)
	topic := topicPrefix + sanitized

	// Ensure topic exists with 30-minute retention
	if err := kafka.EnsureTopic(s.cfg.KafkaBroker, topic); err != nil {
		log.Printf("[%s] WARNING: Failed to ensure topic %s: %v (will rely on auto-create)", sanitized, topic, err)
	}

	runner := stream.NewRunner(cam.ID, sanitized, topic, s.producer, s.cfg.LogLevel)
	backoffIdx := 0
	connected := cam.IsConnected()

	if !connected {
		log.Printf("[%s] Camera is offline (state=%s), will poll hourly for status changes", sanitized, cam.State)
	}

	for {
		if ctx.Err() != nil {
			return
		}

		// --- Offline polling mode ---
		if !connected {
			select {
			case <-time.After(offlinePollInterval):
			case <-ctx.Done():
				return
			}

			connected = s.checkCameraConnected(ctx, cam.ID, sanitized)
			if !connected {
				continue
			}
			log.Printf("[%s] Camera is back online, starting stream", sanitized)
			backoffIdx = 0
			continue // re-enter loop in connected state
		}

		// --- Streaming mode ---
		log.Printf("[%s] Requesting RTSPS stream URL...", sanitized)
		rtspsURL, err := s.apiClient.CreateRTSPSStream(ctx, cam.ID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[%s] Failed to get RTSPS URL: %v", sanitized, err)

			// At max backoff, check if camera went offline
			if backoffIdx >= len(s.cfg.ReconnectBackoff)-1 {
				connected = s.checkCameraConnected(ctx, cam.ID, sanitized)
				if !connected {
					log.Printf("[%s] Camera appears offline, switching to hourly polling", sanitized)
					continue
				}
			}
			s.backoff(ctx, &backoffIdx, sanitized)
			continue
		}

		log.Printf("[%s] Got RTSPS URL, starting stream...", sanitized)

		startTime := time.Now()
		err = runner.Run(ctx, rtspsURL)
		if ctx.Err() != nil {
			return
		}

		// Reset backoff if stream lasted more than 60 seconds (was stable)
		if time.Since(startTime) > 60*time.Second {
			backoffIdx = 0
		}

		log.Printf("[%s] Stream ended: %v", sanitized, err)

		// If stream failed quickly and we're at max backoff, check if camera went offline
		if time.Since(startTime) < 60*time.Second && backoffIdx >= len(s.cfg.ReconnectBackoff)-1 {
			connected = s.checkCameraConnected(ctx, cam.ID, sanitized)
			if !connected {
				log.Printf("[%s] Camera appears offline, switching to hourly polling", sanitized)
				continue
			}
		}

		s.backoff(ctx, &backoffIdx, sanitized)
	}
}

// checkCameraConnected re-fetches cameras from the API and returns whether
// the given camera is in CONNECTED state.
func (s *Service) checkCameraConnected(ctx context.Context, cameraID, name string) bool {
	cameras, err := s.apiClient.FetchCameras(ctx)
	if err != nil {
		log.Printf("[%s] Failed to check camera status: %v (assuming offline)", name, err)
		return false
	}
	cam, ok := cameras[cameraID]
	if !ok {
		log.Printf("[%s] Camera not found in API response (assuming offline)", name)
		return false
	}
	if cam.IsConnected() {
		return true
	}
	log.Printf("[%s] Camera state: %s (offline)", name, cam.State)
	return false
}

// backoff waits for the next backoff duration before retrying.
func (s *Service) backoff(ctx context.Context, idx *int, name string) {
	if *idx >= len(s.cfg.ReconnectBackoff) {
		*idx = len(s.cfg.ReconnectBackoff) - 1
	}
	delay := s.cfg.ReconnectBackoff[*idx]
	*idx++

	log.Printf("[%s] Reconnecting in %v...", name, delay)
	select {
	case <-time.After(delay):
	case <-ctx.Done():
	}
}

// sanitizeName converts a camera name to a Kafka-topic-friendly format.
// Example: "Courtyard" -> "courtyard", "Front Door" -> "front_door"
func sanitizeName(name string) string {
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	return name
}
