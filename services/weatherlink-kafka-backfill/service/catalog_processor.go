package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"

	"weatherlink-kafka-backfill/cache"
	"weatherlink-kafka-backfill/models"
	"weatherlink-kafka-backfill/repository"
)

// CatalogProcessor handles catalog message processing for backfill
type CatalogProcessor struct {
	catalogRepo *repository.CatalogRepository
	orphanRepo  *repository.OrphanRepository
	cache       *cache.Cache
}

// NewCatalogProcessor creates a new CatalogProcessor
func NewCatalogProcessor(catalogRepo *repository.CatalogRepository, orphanRepo *repository.OrphanRepository, cache *cache.Cache) *CatalogProcessor {
	return &CatalogProcessor{
		catalogRepo: catalogRepo,
		orphanRepo:  orphanRepo,
		cache:       cache,
	}
}

// ProcessMessage processes a catalog message
func (p *CatalogProcessor) ProcessMessage(ctx context.Context, msg kafka.Message) error {
	// Each message is a single sensor type object, not a wrapper with sensor_types array
	var sensorTypeData map[string]interface{}
	if err := json.Unmarshal(msg.Value, &sensorTypeData); err != nil {
		// Save orphaned message with parse error
		p.orphanRepo.Save(ctx, msg, 0, 0, "", fmt.Sprintf("failed to parse catalog: %v", err))
		return fmt.Errorf("failed to parse catalog: %w", err)
	}

	sensorType, ok := sensorTypeData["sensor_type"].(float64)
	if !ok {
		// Save orphaned message with missing sensor_type
		p.orphanRepo.Save(ctx, msg, 0, 0, "", "missing sensor_type in catalog message")
		return fmt.Errorf("missing sensor_type in catalog message")
	}

	dataStructures, ok := sensorTypeData["data_structures"].([]interface{})
	if !ok {
		// Save orphaned message with missing data_structures
		p.orphanRepo.Save(ctx, msg, 0, 0, "", "missing data_structures in catalog message")
		return fmt.Errorf("missing data_structures in catalog message")
	}

	entriesProcessed := 0
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
			entriesProcessed++
		}
	}

	log.Printf("Processed %d catalog entries from sensor_type %d", entriesProcessed, int(sensorType))
	return nil
}
