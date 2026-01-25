package service

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"weatherlink-materialize-to-sql/cache"
	"weatherlink-materialize-to-sql/models"
	"weatherlink-materialize-to-sql/repository"
)

// Materializer orchestrates the materialization of Kafka messages to PostgreSQL
type Materializer struct {
	config         models.Config
	pool           *pgxpool.Pool
	cache          *cache.Cache
	metadataProc   *MetadataProcessor
	catalogProc    *CatalogProcessor
	dataProc       *DataProcessor
	enricher       *Enricher
	metadataReader *kafka.Reader
	catalogReader  *kafka.Reader
	batchWriter    *repository.BatchWriter
	workerPool     *WorkerPool
}

// New creates a new Materializer
func New(config models.Config, pool *pgxpool.Pool) (*Materializer, error) {
	// Initialize cache
	c := cache.New()

	// Initialize repositories
	deviceRepo := repository.NewDeviceRepository(pool)
	tagRepo := repository.NewTagRepository(pool)
	catalogRepo := repository.NewCatalogRepository(pool)
	recordRepo := repository.NewRecordRepository(pool)
	orphanRepo := repository.NewOrphanRepository(pool)

	// Initialize batch writer
	flushInterval := time.Duration(config.BatchFlushIntervalMs) * time.Millisecond
	batchWriter := repository.NewBatchWriter(pool, config.BatchSize, flushInterval)

	// Initialize worker pool
	workerPool := NewWorkerPool(
		config.WorkerPoolSize,
		batchWriter,
		tagRepo,
		orphanRepo,
		c,
	)

	// Initialize processors
	metadataProc := NewMetadataProcessor(deviceRepo, orphanRepo, c)
	catalogProc := NewCatalogProcessor(catalogRepo, c)
	dataProc := NewDataProcessor(tagRepo, recordRepo, orphanRepo, c)
	enricher := NewEnricher(tagRepo, c)

	return &Materializer{
		config:       config,
		pool:         pool,
		cache:        c,
		metadataProc: metadataProc,
		catalogProc:  catalogProc,
		dataProc:     dataProc,
		enricher:     enricher,
		batchWriter:  batchWriter,
		workerPool:   workerPool,
	}, nil
}

// Start starts the materializer service
func (m *Materializer) Start(ctx context.Context) error {
	log.Println("Starting ROACH Weather SQL Materializer...")

	// Load initial data
	if err := m.metadataProc.LoadDevices(ctx); err != nil {
		log.Printf("Warning: Failed to load devices: %v", err)
	}

	if err := m.dataProc.LoadTags(ctx); err != nil {
		log.Printf("Warning: Failed to load tags: %v", err)
	}

	if err := m.catalogProc.LoadCatalog(ctx); err != nil {
		log.Printf("Warning: Failed to load catalog: %v", err)
	}

	// Enrich existing tags with catalog metadata after catalog is loaded
	if err := m.enricher.EnrichTags(ctx); err != nil {
		log.Printf("Warning: Failed to enrich tags: %v", err)
	}

	// Start worker pool
	m.workerPool.Start()

	// Start metrics logger
	go m.logMetrics(ctx)

	// Start catalog listener in background
	go m.listenForCatalog(ctx)

	// Start metadata listener in background
	go m.listenForMetadata(ctx)

	// Subscribe to data topics
	log.Println("Subscribing to weather data topics...")
	return m.listenForData(ctx)
}

// Close closes the materializer
func (m *Materializer) Close() error {
	log.Println("Closing materializer...")
	
	// Close Kafka readers
	if m.catalogReader != nil {
		m.catalogReader.Close()
	}
	if m.metadataReader != nil {
		m.metadataReader.Close()
	}
	
	// Shutdown worker pool
	if m.workerPool != nil {
		if err := m.workerPool.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down worker pool: %v", err)
		}
	}
	
	// Close batch writer (flushes remaining records)
	if m.batchWriter != nil {
		if err := m.batchWriter.Close(context.Background()); err != nil {
			log.Printf("Error closing batch writer: %v", err)
		}
	}
	
	// Close connection pool
	if m.pool != nil {
		m.pool.Close()
	}
	
	log.Println("Materializer closed")
	return nil
}

// SetReaders sets the Kafka readers (called after construction)
func (m *Materializer) SetReaders(metadataReader, catalogReader *kafka.Reader) {
	m.metadataReader = metadataReader
	m.catalogReader = catalogReader
}

// listenForMetadata listens for metadata messages
func (m *Materializer) listenForMetadata(ctx context.Context) {
	m.metadataProc.Listen(ctx, m.metadataReader)
}

// listenForCatalog listens for catalog messages
func (m *Materializer) listenForCatalog(ctx context.Context) {
	m.catalogProc.Listen(ctx, m.catalogReader, m.enricher)
}

// listenForData listens for data messages
func (m *Materializer) listenForData(ctx context.Context) error {
	return m.dataProc.ListenWithWorkerPool(ctx, m.config.KafkaBroker, m.workerPool)
}

// logMetrics periodically logs performance metrics
func (m *Materializer) logMetrics(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Get worker pool metrics
			processed, errors := m.workerPool.GetMetrics()

			// Get batch writer metrics
			numericInserts, textInserts, nullInserts, flushes := m.batchWriter.GetMetrics()

			// Get current batch sizes
			numericBatch, textBatch, nullBatch := m.batchWriter.GetBatchSizes()

			// Get pool stats
			stats := m.pool.Stat()

			log.Printf("=== METRICS ===")
			log.Printf("Worker Pool: processed=%d, errors=%d", processed, errors)
			log.Printf("Batch Writer: numeric=%d, text=%d, null=%d, flushes=%d", 
				numericInserts, textInserts, nullInserts, flushes)
			log.Printf("Current Batches: numeric=%d, text=%d, null=%d", 
				numericBatch, textBatch, nullBatch)
			log.Printf("DB Pool: acquired=%d, idle=%d, max=%d", 
				stats.AcquiredConns(), stats.IdleConns(), stats.MaxConns())
			log.Printf("===============")
		}
	}
}
