package service

import (
	"context"
	"database/sql"
	"log"

	"github.com/segmentio/kafka-go"

	"weather-sql/cache"
	"weather-sql/models"
	"weather-sql/repository"
)

// Materializer orchestrates the materialization of Kafka messages to PostgreSQL
type Materializer struct {
	config       models.Config
	db           *sql.DB
	cache        *cache.Cache
	metadataProc *MetadataProcessor
	catalogProc  *CatalogProcessor
	dataProc     *DataProcessor
	enricher     *Enricher
	metadataReader *kafka.Reader
	catalogReader  *kafka.Reader
}

// New creates a new Materializer
func New(config models.Config, db *sql.DB) (*Materializer, error) {
	// Initialize cache
	c := cache.New()

	// Initialize repositories
	deviceRepo := repository.NewDeviceRepository(db)
	tagRepo := repository.NewTagRepository(db)
	catalogRepo := repository.NewCatalogRepository(db)
	recordRepo := repository.NewRecordRepository(db)
	orphanRepo := repository.NewOrphanRepository(db)

	// Initialize processors
	metadataProc := NewMetadataProcessor(deviceRepo, c)
	catalogProc := NewCatalogProcessor(catalogRepo, c)
	dataProc := NewDataProcessor(tagRepo, recordRepo, orphanRepo, c)
	enricher := NewEnricher(tagRepo, c)

	return &Materializer{
		config:       config,
		db:           db,
		cache:        c,
		metadataProc: metadataProc,
		catalogProc:  catalogProc,
		dataProc:     dataProc,
		enricher:     enricher,
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
	if m.catalogReader != nil {
		m.catalogReader.Close()
	}
	if m.metadataReader != nil {
		m.metadataReader.Close()
	}
	if m.db != nil {
		m.db.Close()
	}
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
	return m.dataProc.Listen(ctx, m.config.KafkaBroker)
}
