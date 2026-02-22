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

	"github.com/fsnotify/fsnotify"
	"github.com/segmentio/kafka-go"

	"unifi-smart-archive/models"
)

// StreamSession is an active stream: one per (camera, detection_type); created on first message (with or without end).
// Frames are copied to the archive as they appear in the source dir until the session is complete.
type StreamSession struct {
	CameraName    string
	DetectionType string
	StartMs       int64
	EndMs         int64
	CopyAfterUnix int64
	ArchiveBase   string
	EventIDs      []string
	copied        map[int64]struct{}
}

// WaitingEvent is an event we've seen (start only) and are waiting for a final message with end.
// If no message for this event within EventEndTimeout, we stop waiting and do not archive.
type WaitingEvent struct {
	CameraName    string
	DetectionType string
	StartMs       int64
	LastMessageAt time.Time
}

// Service consumes smart events and streams JPEG frames to the archive in real time.
type Service struct {
	cfg           models.Config
	reader        *kafka.Reader
	watcher       *fsnotify.Watcher
	streams       map[string]*StreamSession // cameraKey(camera, type) -> active stream
	watchedDirs   map[string]int           // camera source dir -> ref count
	waitingForEnd map[string]*WaitingEvent
	done          map[string]struct{}
	mu            sync.Mutex
	workerWg      sync.WaitGroup
}

// New creates a new archive service.
func New(cfg models.Config, reader *kafka.Reader) *Service {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("fsnotify NewWatcher: %v — streaming will use catch-up only", err)
		w = nil
	}
	return &Service{
		cfg:           cfg,
		reader:        reader,
		watcher:       w,
		streams:       make(map[string]*StreamSession),
		watchedDirs:   make(map[string]int),
		waitingForEnd: make(map[string]*WaitingEvent),
		done:          make(map[string]struct{}),
	}
}

// Start runs the consumer loop, the fsnotify handler, and the copy/retention worker.
// Exits on context cancel or Kafka consumer/commit error.
func (s *Service) Start(ctx context.Context) error {
	s.workerWg.Add(1)
	go s.runWorker(ctx)
	if s.watcher != nil {
		s.workerWg.Add(1)
		go s.runWatcher(ctx)
	}

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
		delete(s.waitingForEnd, event.ID)
	}

	key := cameraKey(cameraName, detectionType)
	copyAfterUnix := int64(0)
	if event.HasEnd() {
		copyAfterUnix = event.End/1000 + int64(s.cfg.TrailSeconds) + int64(s.cfg.CopyDelaySeconds)
	}

	existing, ok := s.streams[key]
	if ok {
		// Merge: extend window, update copyAfter when we get end, add event ID
		if event.Start < existing.StartMs {
			// Earlier start: switch archive to new path and migrate
			oldBase := existing.ArchiveBase
			existing.StartMs = event.Start
			existing.ArchiveBase = s.archiveBase(cameraName, detectionType, existing.StartMs)
			if oldBase != existing.ArchiveBase {
				_ = os.MkdirAll(existing.ArchiveBase, 0755)
				s.catchUpLocked(existing)
				_ = os.RemoveAll(oldBase)
			}
		}
		if event.HasEnd() {
			if event.End > existing.EndMs {
				existing.EndMs = event.End
			}
			if copyAfterUnix > existing.CopyAfterUnix {
				existing.CopyAfterUnix = copyAfterUnix
			}
			s.catchUpLocked(existing)
		}
		existing.EventIDs = append(existing.EventIDs, event.ID)
		if s.cfg.LogLevel == "debug" {
			log.Printf("DEBUG: merged event %s into stream %s (camera=%s type=%s)", event.ID, key, cameraName, detectionType)
		}
		return
	}

	// New stream: create session and start streaming
	archiveBase := s.archiveBase(cameraName, detectionType, event.Start)
	if err := os.MkdirAll(archiveBase, 0755); err != nil {
		log.Printf("Failed to create archive dir %s: %v", archiveBase, err)
		return
	}
	session := &StreamSession{
		CameraName:    cameraName,
		DetectionType: detectionType,
		StartMs:       event.Start,
		EndMs:         event.End,
		CopyAfterUnix: copyAfterUnix,
		ArchiveBase:   archiveBase,
		EventIDs:      []string{event.ID},
		copied:        make(map[int64]struct{}),
	}
	s.streams[key] = session
	s.ensureWatchedLocked(cameraName)
	s.catchUpLocked(session)

	if !event.HasEnd() {
		s.waitingForEnd[event.ID] = &WaitingEvent{
			CameraName:    cameraName,
			DetectionType: detectionType,
			StartMs:       event.Start,
			LastMessageAt: now,
		}
	}
	if s.cfg.LogLevel == "debug" {
		log.Printf("DEBUG: started stream %s for event %s (camera=%s type=%s)", key, event.ID, cameraName, detectionType)
	}
}

func (s *Service) archiveBase(cameraName, detectionType string, startMs int64) string {
	startSec := startMs / 1000
	return filepath.Join(s.cfg.ArchiveDir, "smart", detectionType, cameraName, strconv.FormatInt(startSec, 10))
}

// ensureWatchedLocked adds the camera's source dir to the watcher if not already. Call with s.mu held.
func (s *Service) ensureWatchedLocked(cameraName string) {
	if s.watcher == nil {
		return
	}
	dir := filepath.Join(s.cfg.SourceDir, cameraName)
	s.watchedDirs[dir]++
	if s.watchedDirs[dir] > 1 {
		return
	}
	if err := s.watcher.Add(dir); err != nil {
		log.Printf("watcher Add %s: %v", dir, err)
		s.watchedDirs[dir]--
	}
}

// catchUpLocked copies any existing frames in the session's window from source to archive. Call with s.mu held.
func (s *Service) catchUpLocked(session *StreamSession) {
	fromSec := session.StartMs/1000 - int64(s.cfg.LeadSeconds)
	toSec := int64(0)
	if session.EndMs > 0 {
		toSec = session.EndMs/1000 + int64(s.cfg.TrailSeconds)
	} else {
		toSec = time.Now().Unix() + 5
	}
	sourceDir := filepath.Join(s.cfg.SourceDir, session.CameraName)
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jpg") {
			continue
		}
		tsStr := strings.TrimSuffix(e.Name(), ".jpg")
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil || ts < fromSec || ts > toSec {
			continue
		}
		if _, ok := session.copied[ts]; ok {
			continue
		}
		srcPath := filepath.Join(sourceDir, e.Name())
		dstPath := filepath.Join(session.ArchiveBase, e.Name())
		if err := copyFile(srcPath, dstPath); err != nil {
			continue
		}
		session.copied[ts] = struct{}{}
	}
}

// runWatcher handles fsnotify events and copies new files into active streams. Call without s.mu.
func (s *Service) runWatcher(ctx context.Context) {
	defer s.workerWg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-s.watcher.Errors:
			if !ok {
				return
			}
			log.Printf("fsnotify error: %v", err)
		case ev, ok := <-s.watcher.Events:
			if !ok {
				return
			}
			if ev.Op != fsnotify.Create {
				continue
			}
			if !strings.HasSuffix(ev.Name, ".jpg") {
				continue
			}
			dir, name := filepath.Split(strings.TrimSuffix(ev.Name, ".jpg") + ".jpg")
			dir = filepath.Clean(dir)
			tsStr := strings.TrimSuffix(name, ".jpg")
			ts, err := strconv.ParseInt(tsStr, 10, 64)
			if err != nil {
				continue
			}
			cameraName := sanitizeName(filepath.Base(dir))
			s.mu.Lock()
			for _, session := range s.streams {
				if session.CameraName != cameraName {
					continue
				}
				fromSec := session.StartMs/1000 - int64(s.cfg.LeadSeconds)
				toSec := int64(0)
				if session.EndMs > 0 {
					toSec = session.EndMs/1000 + int64(s.cfg.TrailSeconds)
				} else {
					toSec = time.Now().Unix() + 60
				}
				if ts < fromSec || ts > toSec {
					continue
				}
				if _, ok := session.copied[ts]; ok {
					continue
				}
				dstPath := filepath.Join(session.ArchiveBase, name)
				if err := copyFile(ev.Name, dstPath); err != nil {
					continue
				}
				session.copied[ts] = struct{}{}
			}
			s.mu.Unlock()
		}
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

// cameraKey returns a stable key for one pending job per (camera, detection type).
func cameraKey(cameraName, detectionType string) string {
	return cameraName + "|" + detectionType
}

// runWorker runs expire, close completed streams, and retention on a ticker.
func (s *Service) runWorker(ctx context.Context) {
	defer s.workerWg.Done()
	ticker := time.NewTicker(s.cfg.WorkerInterval)
	defer ticker.Stop()

	lastRetention := time.Now()
	retentionInterval := time.Hour
	if s.cfg.ArchiveRetentionDays > 0 {
		retentionInterval = time.Duration(s.cfg.ArchiveRetentionDays) * 24 * time.Hour / 24
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
			s.closeCompletedStreams()
			if time.Since(lastRetention) >= retentionInterval {
				s.runRetentionPass()
				lastRetention = time.Now()
			}
		}
	}
}

// expireWaitingForEnd removes events that had no message within EventEndTimeout. For any such event that
// is the only one in its stream and the stream has no end, removes the stream and deletes the partial archive.
func (s *Service) expireWaitingForEnd() {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := time.Now().Add(-s.cfg.EventEndTimeout)
	for id, ev := range s.waitingForEnd {
		if ev.LastMessageAt.Before(cutoff) {
			delete(s.waitingForEnd, id)
			key := cameraKey(ev.CameraName, ev.DetectionType)
			session, hasStream := s.streams[key]
			if hasStream && session.EndMs == 0 && len(session.EventIDs) == 1 && session.EventIDs[0] == id {
				delete(s.streams, key)
				s.releaseWatchedLocked(ev.CameraName)
				_ = os.RemoveAll(session.ArchiveBase)
				if s.cfg.LogLevel == "debug" {
					log.Printf("DEBUG: no follow-up for event %s within %v — removed stream and partial archive", id, s.cfg.EventEndTimeout)
				}
			} else if s.cfg.LogLevel == "debug" && !hasStream {
				log.Printf("DEBUG: no follow-up for event %s within %v — stopped waiting for end (will not archive)", id, s.cfg.EventEndTimeout)
			}
		}
	}
}

func (s *Service) releaseWatchedLocked(cameraName string) {
	if s.watcher == nil {
		return
	}
	dir := filepath.Join(s.cfg.SourceDir, cameraName)
	s.watchedDirs[dir]--
	if s.watchedDirs[dir] <= 0 {
		delete(s.watchedDirs, dir)
		_ = s.watcher.Remove(dir)
	}
}

// closeCompletedStreams removes streams that have end and have copied through toSec and time >= CopyAfterUnix.
func (s *Service) closeCompletedStreams() {
	s.mu.Lock()
	now := time.Now().Unix()
	var toClose []string
	for key, session := range s.streams {
		if session.EndMs == 0 {
			continue
		}
		toSec := session.EndMs/1000 + int64(s.cfg.TrailSeconds)
		if now < session.CopyAfterUnix {
			continue
		}
		if _, ok := session.copied[toSec]; ok {
			toClose = append(toClose, key)
			continue
		}
		s.catchUpLocked(session)
		if _, ok := session.copied[toSec]; ok {
			toClose = append(toClose, key)
			continue
		}
		// After copy delay, last frame may still be missing (gap or writer lag). Close anyway.
		srcPath := filepath.Join(s.cfg.SourceDir, session.CameraName, fmt.Sprintf("%d.jpg", toSec))
		if _, err := os.Stat(srcPath); err != nil {
			toClose = append(toClose, key)
		}
	}
	for _, key := range toClose {
		session := s.streams[key]
		delete(s.streams, key)
		s.releaseWatchedLocked(session.CameraName)
		for _, id := range session.EventIDs {
			s.done[id] = struct{}{}
		}
		eventDesc := strconv.FormatInt(session.StartMs/1000, 10)
		if len(session.EventIDs) > 1 {
			eventDesc = eventDesc + " (" + strconv.Itoa(len(session.EventIDs)) + " events)"
		}
		log.Printf("Archived %s/%s %s: %d frames -> %s", session.DetectionType, session.CameraName, eventDesc, len(session.copied), session.ArchiveBase)
	}
	s.mu.Unlock()
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

// Close releases the watcher and Kafka reader and waits for workers.
func (s *Service) Close() error {
	s.workerWg.Wait()
	if s.watcher != nil {
		_ = s.watcher.Close()
	}
	if s.reader != nil {
		return s.reader.Close()
	}
	return nil
}
