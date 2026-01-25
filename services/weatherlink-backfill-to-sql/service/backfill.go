package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"weatherlink-backfill-to-sql/cache"
	"weatherlink-backfill-to-sql/config"
	"weatherlink-backfill-to-sql/repository"
)

// BackfillService manages the Kafka to DB backfill process
type BackfillService struct {
	config      config.Config
	pool        *pgxpool.Pool
	cache       *cache.Cache
	batchWriter *repository.BatchWriter
	workerPool  *WorkerPool
	deviceRepo  *repository.DeviceRepository
	tagRepo     *repository.TagRepository
	catalogRepo *repository.CatalogRepository
	orphanRepo  *repository.OrphanRepository
	
	// Progress tracking
	progressMutex sync.Mutex
	topicProgress map[string]*TopicProgress
}

// TopicProgress tracks progress for a single topic
type TopicProgress struct {
	Topic        string
	StartOffset  int64
	EndOffset    int64
	CurrentOffset int64
	MessagesProcessed int64
}

// New creates a new BackfillService
func New(cfg config.Config, pool *pgxpool.Pool) (*BackfillService, error) {
	// Initialize cache
	c := cache.New()

	// Initialize repositories
	deviceRepo := repository.NewDeviceRepository(pool)
	tagRepo := repository.NewTagRepository(pool)
	catalogRepo := repository.NewCatalogRepository(pool)
	orphanRepo := repository.NewOrphanRepository(pool)

	// Initialize batch writer with backfill-optimized settings
	flushInterval := time.Duration(cfg.BatchFlushIntervalMs) * time.Millisecond
	batchWriter := repository.NewBatchWriter(pool, cfg.BatchSize, flushInterval)

	// Initialize worker pool
	workerPool := NewWorkerPool(
		cfg.WorkerPoolSize,
		batchWriter,
		tagRepo,
		orphanRepo,
		c,
	)

	return &BackfillService{
		config:        cfg,
		pool:          pool,
		cache:         c,
		batchWriter:   batchWriter,
		workerPool:    workerPool,
		deviceRepo:    deviceRepo,
		tagRepo:       tagRepo,
		catalogRepo:   catalogRepo,
		orphanRepo:    orphanRepo,
		topicProgress: make(map[string]*TopicProgress),
	}, nil
}

// Start starts the backfill process
func (s *BackfillService) Start(ctx context.Context) error {
	log.Println("Loading initial data...")

	// Load devices
	devices, err := s.deviceRepo.LoadAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to load devices: %w", err)
	}
	for _, device := range devices {
		s.cache.SetDevice(device.LSID, device)
	}
	log.Printf("Loaded %d devices into cache", len(devices))

	// Load tags
	tags, err := s.tagRepo.LoadAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to load tags: %w", err)
	}
	for _, tag := range tags {
		s.cache.SetTag(tag.DeviceID, tag.TagName, tag)
	}
	log.Printf("Loaded %d tags into cache", len(tags))

	// Load catalog
	catalog, err := s.catalogRepo.LoadAll(ctx)
	if err != nil {
		log.Printf("Warning: Failed to load catalog: %v", err)
	} else {
		for _, fieldMeta := range catalog {
			s.cache.SetCatalogMetadata(fieldMeta.SensorType, fieldMeta.DataStructureType, fieldMeta.FieldName, fieldMeta)
		}
		log.Printf("Loaded %d catalog entries into cache", len(catalog))
	}

	// Phase 1: Process metadata topics if requested
	if s.config.IncludeMetadata {
		log.Println("Processing metadata topics...")
		if err := s.processMetadataPhase(ctx); err != nil {
			return fmt.Errorf("metadata phase failed: %w", err)
		}
		
		// Reload cache after metadata backfill
		log.Println("Reloading devices and catalog into cache...")
		devices, err = s.deviceRepo.LoadAll(ctx)
		if err != nil {
			return fmt.Errorf("failed to reload devices: %w", err)
		}
		for _, device := range devices {
			s.cache.SetDevice(device.LSID, device)
		}
		log.Printf("Reloaded %d devices into cache", len(devices))
		
		catalog, err = s.catalogRepo.LoadAll(ctx)
		if err != nil {
			log.Printf("Warning: Failed to reload catalog: %v", err)
		} else {
			for _, fieldMeta := range catalog {
				s.cache.SetCatalogMetadata(fieldMeta.SensorType, fieldMeta.DataStructureType, fieldMeta.FieldName, fieldMeta)
			}
			log.Printf("Reloaded %d catalog entries into cache", len(catalog))
		}
	}

	// Start worker pool
	s.workerPool.Start()

	// Start progress logger
	go s.logProgress(ctx)

	// Phase 2: Process data topics
	return s.processTopics(ctx)
}

// processTopics processes all configured topics in parallel
func (s *BackfillService) processTopics(ctx context.Context) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(s.config.Topics))

	// Process each topic in parallel
	for _, topic := range s.config.Topics {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			if err := s.processTopic(ctx, t); err != nil {
				log.Printf("Error processing topic %s: %v", t, err)
				errChan <- err
			}
		}(topic)
	}

	// Wait for all topics to complete
	wg.Wait()
	close(errChan)

	// Shutdown worker pool
	log.Println("Shutting down worker pool...")
	if err := s.workerPool.Shutdown(ctx); err != nil {
		log.Printf("Error shutting down worker pool: %v", err)
	}

	// Flush remaining batches
	log.Println("Flushing final batches...")
	if err := s.batchWriter.FlushAll(ctx); err != nil {
		log.Printf("Error flushing final batches: %v", err)
	}

	// Report final statistics
	s.reportFinalStats()

	// Check for errors
	var firstErr error
	for err := range errChan {
		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// processTopic processes a single topic from start to end offset
func (s *BackfillService) processTopic(ctx context.Context, topic string) error {
	log.Printf("Processing topic: %s", topic)

	// Determine partition to read from (default to 0)
	partition := 0

	// Create reader for specific topic partition (no consumer group)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{s.config.KafkaBroker},
		Topic:     topic,
		Partition: partition,
		MinBytes:  10e3,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	// Set starting offset based on config
	if s.config.StartOffset == -2 || s.config.StartOffset == 0 {
		// Start from beginning
		if err := reader.SetOffset(kafka.FirstOffset); err != nil {
			return fmt.Errorf("failed to set offset to first: %w", err)
		}
	} else if s.config.StartOffset == -1 {
		// Start from end
		if err := reader.SetOffset(kafka.LastOffset); err != nil {
			return fmt.Errorf("failed to set offset to last: %w", err)
		}
	} else {
		// Start from specific offset
		if err := reader.SetOffset(s.config.StartOffset); err != nil {
			return fmt.Errorf("failed to set offset to %d: %w", s.config.StartOffset, err)
		}
	}

	log.Printf("Topic %s: set offset to %s", topic, formatOffsetForLog(s.config.StartOffset))

	// Initialize progress tracking
	progress := &TopicProgress{
		Topic:             topic,
		StartOffset:       0,
		EndOffset:         s.config.EndOffset,
		CurrentOffset:     0,
		MessagesProcessed: 0,
	}
	s.progressMutex.Lock()
	s.topicProgress[topic] = progress
	s.progressMutex.Unlock()

	log.Printf("Topic %s: starting backfill", topic)

	// Process messages
	messageCount := int64(0)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Read message with timeout
			msgCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			msg, err := reader.FetchMessage(msgCtx)
			cancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					// No more messages available
					log.Printf("Topic %s: processed %d messages", topic, messageCount)
					return nil
				}
				return fmt.Errorf("failed to fetch message: %w", err)
			}

			// Update start offset from first message
			if messageCount == 0 {
				progress.StartOffset = msg.Offset
			}

			// Check if we've reached the end offset (if specified)
			if s.config.EndOffset >= 0 && msg.Offset >= s.config.EndOffset {
				log.Printf("Topic %s: reached end offset %d", topic, s.config.EndOffset)
				return nil
			}

			// Submit to worker pool
			if err := s.workerPool.SubmitMessage(msg); err != nil {
				log.Printf("Error submitting message to worker pool: %v", err)
				continue
			}

			// Update progress
			messageCount++
			s.progressMutex.Lock()
			progress.CurrentOffset = msg.Offset + 1
			progress.MessagesProcessed++
			s.progressMutex.Unlock()
		}
	}
}

// logProgress periodically logs progress
func (s *BackfillService) logProgress(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.progressMutex.Lock()
			for _, progress := range s.topicProgress {
				totalMessages := progress.EndOffset - progress.StartOffset
				if totalMessages <= 0 {
					continue
				}
				pct := float64(progress.CurrentOffset-progress.StartOffset) / float64(totalMessages) * 100
				log.Printf("Progress: %s %d/%d (%.1f%%)", 
					progress.Topic, 
					progress.CurrentOffset-progress.StartOffset,
					totalMessages,
					pct)
			}
			s.progressMutex.Unlock()
		}
	}
}

// reportFinalStats reports final statistics
func (s *BackfillService) reportFinalStats() {
	processed, errors := s.workerPool.GetMetrics()
	numericInserts, textInserts, nullInserts, flushes := s.batchWriter.GetMetrics()

	// Get orphaned message count
	ctx := context.Background()
	var orphanCount int64
	err := s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM orphaned_messages WHERE NOT reprocessed").Scan(&orphanCount)
	if err != nil {
		log.Printf("Warning: Failed to get orphan count: %v", err)
	}

	log.Println("=== BACKFILL COMPLETE ===")
	log.Printf("Messages processed: %d", processed)
	log.Printf("Processing errors: %d", errors)
	log.Printf("Records inserted: numeric=%d, text=%d, null=%d", 
		numericInserts, textInserts, nullInserts)
	log.Printf("Total batch flushes: %d", flushes)
	if orphanCount > 0 {
		log.Printf("Orphaned messages: %d (check orphaned_messages table)", orphanCount)
	}
	log.Println("========================")
}

// formatOffsetForLog formats an offset value for logging
func formatOffsetForLog(offset int64) string {
	switch offset {
	case -2:
		return "earliest"
	case -1:
		return "latest"
	case 0:
		return "earliest (0)"
	default:
		return fmt.Sprintf("%d", offset)
	}
}

// Close closes the backfill service
func (s *BackfillService) Close() error {
	log.Println("Closing backfill service...")

	if s.batchWriter != nil {
		if err := s.batchWriter.Close(context.Background()); err != nil {
			log.Printf("Error closing batch writer: %v", err)
		}
	}

	log.Println("Backfill service closed")
	return nil
}

// processMetadataPhase processes metadata topics in order
func (s *BackfillService) processMetadataPhase(ctx context.Context) error {
	// Create processors
	metadataProcessor := NewMetadataProcessor(s.deviceRepo, s.orphanRepo, s.cache)
	catalogProcessor := NewCatalogProcessor(s.catalogRepo, s.orphanRepo, s.cache)

	// Process metadata topics in foreign-key dependency order
	metadataTopics := []struct {
		topic     string
		processor interface{}
	}{
		{"weather.metadata.sensors", metadataProcessor},
		{"weather.metadata.catalog", catalogProcessor},
		{"weather.metadata.station", metadataProcessor},
	}

	for _, mt := range metadataTopics {
		if err := s.processMetadataTopic(ctx, mt.topic, mt.processor); err != nil {
			return fmt.Errorf("failed to process topic %s: %w", mt.topic, err)
		}
	}

	return nil
}

// processMetadataTopic processes a single metadata topic synchronously
func (s *BackfillService) processMetadataTopic(ctx context.Context, topic string, processor interface{}) error {
	log.Printf("Processing metadata topic: %s", topic)

	// Determine partition to read from (default to 0)
	partition := 0

	// Create reader for specific topic partition (no consumer group)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{s.config.KafkaBroker},
		Topic:     topic,
		Partition: partition,
		MinBytes:  1,    // Read even single bytes for metadata
		MaxBytes:  10e6,
	})
	defer reader.Close()

	// Set starting offset based on config
	if s.config.StartOffset == -2 || s.config.StartOffset == 0 {
		// Start from beginning
		if err := reader.SetOffset(kafka.FirstOffset); err != nil {
			return fmt.Errorf("failed to set offset to first: %w", err)
		}
	} else if s.config.StartOffset == -1 {
		// Start from end
		if err := reader.SetOffset(kafka.LastOffset); err != nil {
			return fmt.Errorf("failed to set offset to last: %w", err)
		}
	} else {
		// Start from specific offset
		if err := reader.SetOffset(s.config.StartOffset); err != nil {
			return fmt.Errorf("failed to set offset to %d: %w", s.config.StartOffset, err)
		}
	}

	log.Printf("Topic %s: set offset to %s", topic, formatOffsetForLog(s.config.StartOffset))

	// Process messages
	messageCount := int64(0)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Read message with timeout
			msgCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			msg, err := reader.FetchMessage(msgCtx)
			cancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					// No more messages available
					log.Printf("Topic %s: processed %d messages", topic, messageCount)
					return nil
				}
				return fmt.Errorf("failed to fetch message: %w", err)
			}

			// Check if we've reached the end offset (if specified)
			if s.config.EndOffset >= 0 && msg.Offset >= s.config.EndOffset {
				log.Printf("Topic %s: reached end offset %d", topic, s.config.EndOffset)
				return nil
			}

			// Process message based on processor type
			var processErr error
			switch p := processor.(type) {
			case *MetadataProcessor:
				processErr = p.ProcessMessage(ctx, msg)
			case *CatalogProcessor:
				processErr = p.ProcessMessage(ctx, msg)
			default:
				processErr = fmt.Errorf("unknown processor type")
			}

			if processErr != nil {
				log.Printf("Warning: Error processing message from %s: %v", topic, processErr)
				// Orphan is already saved by processor, continue processing other messages
			}

			messageCount++
		}
	}
}

