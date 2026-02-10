package service

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"unifi-video-jpg/api"
	"unifi-video-jpg/models"
	"unifi-video-jpg/stream"
)

const (
	retentionCheckInterval = 1 * time.Minute
	offlinePollInterval    = 1 * time.Hour
)

// Service orchestrates per-camera RTSPS streaming to the filesystem.
type Service struct {
	cfg       models.Config
	apiClient *api.Client
}

// New creates a new Service instance.
func New(cfg models.Config, apiClient *api.Client) *Service {
	return &Service{
		cfg:       cfg,
		apiClient: apiClient,
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
// Retention cleanup runs in a goroutine for this camera's output directory.
func (s *Service) runCamera(ctx context.Context, cam models.CameraInfo) {
	sanitized := sanitizeName(cam.Name)
	outputDir := filepath.Join(s.cfg.JPGOutputDir, sanitized)

	// Start retention cleanup for this camera's directory
	go runRetention(ctx, outputDir, s.cfg.Retention)

	runner := stream.NewRunner(cam.ID, sanitized, outputDir, s.cfg.LogLevel)
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
			continue
		}

		// --- Streaming mode ---
		log.Printf("[%s] Requesting RTSPS stream URL...", sanitized)
		rtspsURL, err := s.apiClient.CreateRTSPSStream(ctx, cam.ID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[%s] Failed to get RTSPS URL: %v", sanitized, err)

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

		if time.Since(startTime) > 60*time.Second {
			backoffIdx = 0
		}

		log.Printf("[%s] Stream ended: %v", sanitized, err)

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

// runRetention periodically deletes JPEG files in dir older than retention.
// Runs until ctx is cancelled.
func runRetention(ctx context.Context, dir string, retention time.Duration) {
	ticker := time.NewTicker(retentionCheckInterval)
	defer ticker.Stop()

	doCleanup := func() {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return // dir may not exist yet
		}
		now := time.Now().Unix()
		cutoff := now - int64(retention.Seconds())
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if !strings.HasSuffix(name, ".jpg") {
				continue
			}
			tsStr := strings.TrimSuffix(name, ".jpg")
			ts, err := strconv.ParseInt(tsStr, 10, 64)
			if err != nil {
				continue
			}
			if ts < cutoff {
				path := filepath.Join(dir, name)
				if err := os.Remove(path); err != nil {
					log.Printf("Retention: remove %s: %v", path, err)
				}
			}
		}
	}

	// Run once immediately, then on ticker
	doCleanup()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			doCleanup()
		}
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

// sanitizeName converts a camera name to a filesystem-safe subdirectory name.
// Example: "Courtyard" -> "courtyard", "Front Door" -> "front_door"
func sanitizeName(name string) string {
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	return name
}
