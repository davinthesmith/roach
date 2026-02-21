package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"unifi-smart-archive/models"
)

// PendingJob is a copy job scheduled for after copyAfterUnix.
type PendingJob struct {
	CameraName    string
	DetectionType string
	StartMs       int64
	EndMs         int64
	CopyAfterUnix int64
}

// WaitingEvent is an event we've seen (start only) and are waiting for a final message with end.
// If no message for this event within EventEndTimeout, we stop waiting and do not archive.
type WaitingEvent struct {
	CameraName    string
	DetectionType string
	StartMs       int64
	LastMessageAt time.Time
}

// Service consumes smart events and copies JPEG windows to the archive.
type Service struct {
	cfg           models.Config
	reader        *kafka.Reader
	pending       map[string]*PendingJob  // event ID -> copy job (has end)
	waitingForEnd map[string]*WaitingEvent // event ID -> waiting for final message with end
	done          map[string]struct{}     // event IDs already copied (avoid double copy on redelivery)
	mu            sync.Mutex
	workerWg      sync.WaitGroup
}

// New creates a new archive service.
func New(cfg models.Config, reader *kafka.Reader) *Service {
	return &Service{
		cfg:           cfg,
		reader:        reader,
		pending:       make(map[string]*PendingJob),
		waitingForEnd: make(map[string]*WaitingEvent),
		done:          make(map[string]struct{}),
	}
}

// Start runs the consumer loop and starts the copy/retention worker.
// Exits on context cancel or Kafka consumer/commit error.
func (s *Service) Start(ctx context.Context) error {
	// Start copy + retention worker
	s.workerWg.Add(1)
	go s.runWorker(ctx)

	log.Printf("Consuming smart events from %q (group: %s); event end timeout: %v (stop archiving if no follow-up)",
		s.cfg.KafkaTopic, s.cfg.KafkaConsumerGroup, s.cfg.EventEndTimeout)

	for {
		msg, err := s.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("Kafka error: %v — exiting", err)
			return fmt.Errorf("kafka consumer: %w", err)
		}

		s.processMessage(ctx, msg)

		if err := s.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Kafka commit error: %v — exiting", err)
			return fmt.Errorf("kafka commit: %w", err)
		}
	}
}

func (s *Service) processMessage(ctx context.Context, msg kafka.Message) {
	var event models.SmartEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("Failed to parse smart event: %v — body: %s", err, string(msg.Value))
		return
	}
	if event.ID == "" || event.Start <= 0 {
		return
	}

	cameraName := getHeader(msg, "camera_name")
	if cameraName == "" {
		if idx := strings.IndexByte(string(msg.Key), ':'); idx > 0 {
			cameraName = string(msg.Key[:idx])
		}
	}
	if cameraName == "" {
		log.Printf("Event %s: missing camera_name, skipping", event.ID)
		return
	}
	cameraName = sanitizeName(cameraName)

	detectionType := ""
	if len(event.SmartDetectTypes) > 0 {
		detectionType = event.SmartDetectTypes[0]
	}
	if detectionType == "" {
		detectionType = getHeader(msg, "detection_type")
	}
	if detectionType == "" {
		detectionType = "unknown"
	}
	detectionType = sanitizeName(detectionType)

	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, already := s.done[event.ID]; already {
		return
	}

	if event.HasEnd() {
		// Final message: schedule copy and stop waiting for this event
		delete(s.waitingForEnd, event.ID)
		copyAfterUnix := event.End/1000 + int64(s.cfg.TrailSeconds) + int64(s.cfg.CopyDelaySeconds)
		s.pending[event.ID] = &PendingJob{
			CameraName:    cameraName,
			DetectionType: detectionType,
			StartMs:       event.Start,
			EndMs:         event.End,
			CopyAfterUnix: copyAfterUnix,
		}
		if s.cfg.LogLevel == "debug" {
			log.Printf("DEBUG: scheduled copy for event %s at %d (camera=%s type=%s)",
				event.ID, copyAfterUnix, cameraName, detectionType)
		}
		return
	}

	// No end yet: we're waiting for the final message. Record that we got a message for this event.
	s.waitingForEnd[event.ID] = &WaitingEvent{
		CameraName:    cameraName,
		DetectionType: detectionType,
		StartMs:       event.Start,
		LastMessageAt: now,
	}
	if s.cfg.LogLevel == "debug" {
		log.Printf("DEBUG: event %s has no end yet, waiting for final message", event.ID)
	}
}

func getHeader(msg kafka.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func sanitizeName(name string) string {
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, "-", "_")
	return name
}

// runWorker runs the copy and retention passes on a ticker.
func (s *Service) runWorker(ctx context.Context) {
	defer s.workerWg.Done()
	ticker := time.NewTicker(s.cfg.WorkerInterval)
	defer ticker.Stop()

	lastRetention := time.Now()
	retentionInterval := time.Hour
	if s.cfg.ArchiveRetentionDays > 0 {
		retentionInterval = time.Duration(s.cfg.ArchiveRetentionDays) * 24 * time.Hour / 24 // at least once per day
		if retentionInterval > time.Hour {
			retentionInterval = time.Hour
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.expireWaitingForEnd()
			s.runCopyPass()
			if time.Since(lastRetention) >= retentionInterval {
				s.runRetentionPass()
				lastRetention = time.Now()
			}
		}
	}
}

// expireWaitingForEnd removes events that have had no message within EventEndTimeout (stop archiving those).
func (s *Service) expireWaitingForEnd() {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := time.Now().Add(-s.cfg.EventEndTimeout)
	for id, ev := range s.waitingForEnd {
		if ev.LastMessageAt.Before(cutoff) {
			delete(s.waitingForEnd, id)
			if s.cfg.LogLevel == "debug" {
				log.Printf("DEBUG: no follow-up for event %s within %v — stopped waiting for end (will not archive)", id, s.cfg.EventEndTimeout)
			}
		}
	}
}

func (s *Service) runCopyPass() {
	s.mu.Lock()
	now := time.Now().Unix()
	var toRun []*PendingJob
	var ids []string
	for id, job := range s.pending {
		if now >= job.CopyAfterUnix {
			toRun = append(toRun, job)
			ids = append(ids, id)
		}
	}
	for _, id := range ids {
		delete(s.pending, id)
	}
	s.mu.Unlock()

	for i, job := range toRun {
		s.copyWindow(job)
		s.mu.Lock()
		s.done[ids[i]] = struct{}{}
		s.mu.Unlock()
	}
}

func (s *Service) copyWindow(job *PendingJob) {
	startSec := job.StartMs / 1000
	endSec := job.EndMs / 1000
	fromSec := startSec - int64(s.cfg.LeadSeconds)
	toSec := endSec + int64(s.cfg.TrailSeconds)

	sourceDir := filepath.Join(s.cfg.SourceDir, job.CameraName)
	archiveBase := filepath.Join(s.cfg.ArchiveDir, "smart", job.DetectionType, job.CameraName, strconv.FormatInt(startSec, 10))
	_ = os.MkdirAll(archiveBase, 0755)

	copied := 0
	for ts := fromSec; ts <= toSec; ts++ {
		srcPath := filepath.Join(sourceDir, fmt.Sprintf("%d.jpg", ts))
		dstPath := filepath.Join(archiveBase, fmt.Sprintf("%d.jpg", ts))
		if err := copyFile(srcPath, dstPath); err != nil {
			continue // skip missing or unreadable
		}
		copied++
	}
	log.Printf("Archived event %s/%s %s: %d frames -> %s", job.DetectionType, job.CameraName, strconv.FormatInt(startSec, 10), copied, archiveBase)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Sync()
}

// runRetentionPass deletes archive event directories older than ARCHIVE_RETENTION_DAYS.
func (s *Service) runRetentionPass() {
	base := filepath.Join(s.cfg.ArchiveDir, "smart")
	entries, err := os.ReadDir(base)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Printf("Retention: read dir %s: %v", base, err)
		return
	}
	cutoff := time.Now().AddDate(0, 0, -s.cfg.ArchiveRetentionDays)
	var removed int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		detectionDir := filepath.Join(base, e.Name())
		s.removeOldEventDirs(detectionDir, cutoff, &removed)
	}
	if removed > 0 {
		log.Printf("Retention: removed %d event directories older than %d days", removed, s.cfg.ArchiveRetentionDays)
	}
}

func (s *Service) removeOldEventDirs(dir string, cutoff time.Time, count *int) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		cameraDir := filepath.Join(dir, e.Name())
		s.removeOldEventDirsIn(cameraDir, cutoff, count)
	}
}

func (s *Service) removeOldEventDirsIn(cameraDir string, cutoff time.Time, count *int) {
	entries, err := os.ReadDir(cameraDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		eventDir := filepath.Join(cameraDir, e.Name())
		info, err := os.Stat(eventDir)
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			if err := os.RemoveAll(eventDir); err != nil {
				log.Printf("Retention: remove %s: %v", eventDir, err)
			} else {
				*count++
			}
		}
	}
}

// Close releases the Kafka reader and waits for the worker.
func (s *Service) Close() error {
	s.workerWg.Wait()
	if s.reader != nil {
		return s.reader.Close()
	}
	return nil
}
