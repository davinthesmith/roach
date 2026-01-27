package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"weatherlink-sql/cache"
	"weatherlink-sql/models"
	"weatherlink-sql/repository"
)

// CatalogProcessor handles catalog message processing
type CatalogProcessor struct {
	catalogRepo *repository.CatalogRepository
	cache       *cache.Cache
}

// NewCatalogProcessor creates a new CatalogProcessor
func NewCatalogProcessor(catalogRepo *repository.CatalogRepository, cache *cache.Cache) *CatalogProcessor {
	return &CatalogProcessor{
		catalogRepo: catalogRepo,
		cache:       cache,
	}
}

// LoadCatalog loads catalog from database into cache
func (p *CatalogProcessor) LoadCatalog(ctx context.Context) error {
	entries, err := p.catalogRepo.LoadAll(ctx)
	if err != nil {
		return err
	}

	if entries == nil {
		return nil
	}

	for _, entry := range entries {
		p.cache.SetCatalogMetadata(entry.SensorType, entry.DataStructureType, getFieldNameFromEntry(entry), entry)
	}

	log.Printf("Loaded %d catalog entries into cache", len(entries))
	return nil
}

// ProcessMessage processes a catalog message
func (p *CatalogProcessor) ProcessMessage(ctx context.Context, msg kafka.Message) error {
	var catalogData map[string]interface{}
	if err := json.Unmarshal(msg.Value, &catalogData); err != nil {
		return fmt.Errorf("failed to parse catalog: %w", err)
	}

	if sensorTypes, ok := catalogData["sensor_types"].([]interface{}); ok {
		log.Printf("Processing catalog with %d sensor types", len(sensorTypes))
		for _, st := range sensorTypes {
			sensorTypeMap, ok := st.(map[string]interface{})
			if !ok {
				continue
			}
			if err := p.processSensorType(ctx, sensorTypeMap); err != nil {
				log.Printf("Warning: %v", err)
			}
		}
	} else if _, ok := catalogData["sensor_type"]; ok {
		// Support per-sensor_type messages published by weatherlink-kafka.
		if err := p.processSensorType(ctx, catalogData); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing sensor_types in catalog")
	}

	log.Println("Catalog processing complete")
	return nil
}

// Listen listens for catalog messages
func (p *CatalogProcessor) Listen(ctx context.Context, reader *kafka.Reader, enricher *Enricher) {
	log.Println("Starting catalog listener...")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				log.Printf("Error fetching catalog message: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if err := p.ProcessMessage(ctx, msg); err != nil {
				log.Printf("Error processing catalog message: %v", err)
			} else {
				// Enrich tags after successful catalog update
				if err := enricher.EnrichTags(ctx); err != nil {
					log.Printf("Warning: Failed to enrich tags after catalog update: %v", err)
				}
			}

			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Error committing catalog message: %v", err)
			}
		}
	}
}

// processSensorType handles a single sensor_type payload.
func (p *CatalogProcessor) processSensorType(ctx context.Context, sensorTypeMap map[string]interface{}) error {
	sensorType, ok := sensorTypeMap["sensor_type"].(float64)
	if !ok {
		return fmt.Errorf("missing sensor_type in catalog message")
	}

	dataStructures, ok := sensorTypeMap["data_structures"].([]interface{})
	if !ok {
		return fmt.Errorf("missing data_structures in catalog message")
	}

	for _, ds := range dataStructures {
		dsMap, ok := ds.(map[string]interface{})
		if !ok {
			continue
		}

		dsType, ok := dsMap["data_structure_type"].(string)
		if !ok {
			continue
		}

		dsDescription, _ := dsMap["description"].(string)
		dataStructure, ok := dsMap["data_structure"].(map[string]interface{})
		if !ok {
			continue
		}

		// Process each field in the data structure
		for fieldName, fieldDef := range dataStructure {
			fieldDefMap, ok := fieldDef.(map[string]interface{})
			if !ok {
				continue
			}

			fieldType, _ := fieldDefMap["type"].(string)
			units, _ := fieldDefMap["units"].(string)

			// Store in database
			if err := p.catalogRepo.Upsert(ctx, int(sensorType), dsType, fieldName, fieldType, units, dsDescription); err != nil {
				log.Printf("Warning: %v", err)
				continue
			}

			// Update in-memory cache with complete metadata
			metadata := &models.FieldMetadata{
				FieldName:         fieldName,
				FieldType:         fieldType,
				Units:             units,
				Description:       dsDescription,
				DataStructureType: dsType,
				SensorType:        int(sensorType),
				RawMetadata:       fieldDefMap, // Store complete field definition
			}
			p.cache.SetCatalogMetadata(int(sensorType), dsType, fieldName, metadata)
		}
	}

	return nil
}

// Helper to extract field name from catalog entry
func getFieldNameFromEntry(entry *models.FieldMetadata) string {
	// The FieldName is now directly available in the FieldMetadata struct
	return entry.FieldName
}
