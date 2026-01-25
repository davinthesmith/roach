package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"weatherlink-sql/cache"
	"weatherlink-sql/repository"
)

// Enricher handles tag enrichment with catalog metadata
type Enricher struct {
	tagRepo *repository.TagRepository
	cache   *cache.Cache
}

// NewEnricher creates a new Enricher
func NewEnricher(tagRepo *repository.TagRepository, cache *cache.Cache) *Enricher {
	return &Enricher{
		tagRepo: tagRepo,
		cache:   cache,
	}
}

// EnrichTags backfills existing tags with metadata from catalog
func (e *Enricher) EnrichTags(ctx context.Context) error {
	log.Println("Enriching existing tags with catalog metadata...")

	// Query tags that need enrichment (missing unit, description, or metadata)
	tags, err := e.tagRepo.FindTagsNeedingEnrichment(ctx)
	if err != nil {
		log.Printf("Warning: Failed to query tags for enrichment: %v", err)
		return err
	}

	if len(tags) == 0 {
		return nil
	}

	enrichedCount := 0
	for _, tagInfo := range tags {
		if tagInfo.DataStructureType == nil {
			continue
		}

		// Get catalog metadata for this tag
		fieldMeta := e.cache.GetCatalogMetadata(tagInfo.SensorType, *tagInfo.DataStructureType, tagInfo.TagName)
		if fieldMeta == nil {
			continue
		}

		// Build metadata JSON
		metadata := map[string]interface{}{
			"field_type":          fieldMeta.FieldType,
			"data_structure_type": fieldMeta.DataStructureType,
			"sensor_type":         fieldMeta.SensorType,
		}
		// Merge in raw metadata
		for k, v := range fieldMeta.RawMetadata {
			metadata[k] = v
		}

		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			log.Printf("Warning: Failed to marshal metadata for tag %d: %v", tagInfo.TagID, err)
			continue
		}

		// Update tag with enriched data
		if err := e.tagRepo.Enrich(ctx, tagInfo.TagID, fieldMeta.Units, fieldMeta.Description, metadataJSON); err != nil {
			log.Printf("Warning: Failed to enrich tag %d: %v", tagInfo.TagID, err)
			continue
		}

		enrichedCount++

		// Update cache
		tag := e.cache.GetTag(tagInfo.DeviceID, tagInfo.TagName)
		if tag != nil {
			tag.Unit = &fieldMeta.Units
			tag.Description = &fieldMeta.Description
			e.cache.SetTag(tagInfo.DeviceID, tagInfo.TagName, tag)
		}
	}

	if enrichedCount > 0 {
		log.Printf("Enriched %d tags with catalog metadata", enrichedCount)
	}

	return nil
}

// EnrichTag enriches a single tag with catalog metadata
func (e *Enricher) EnrichTag(ctx context.Context, tagID, deviceID int, tagName string, sensorType int, dataStructureType int) error {
	// Get catalog metadata for this tag
	fieldMeta := e.cache.GetCatalogMetadata(sensorType, dataStructureType, tagName)
	if fieldMeta == nil {
		return fmt.Errorf("no catalog metadata found for tag %s", tagName)
	}

	// Build metadata JSON
	metadata := map[string]interface{}{
		"field_type":          fieldMeta.FieldType,
		"data_structure_type": fieldMeta.DataStructureType,
		"sensor_type":         fieldMeta.SensorType,
	}
	// Merge in raw metadata
	for k, v := range fieldMeta.RawMetadata {
		metadata[k] = v
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata for tag %d: %w", tagID, err)
	}

	// Update tag with enriched data
	if err := e.tagRepo.Enrich(ctx, tagID, fieldMeta.Units, fieldMeta.Description, metadataJSON); err != nil {
		return fmt.Errorf("failed to enrich tag %d: %w", tagID, err)
	}

	// Update cache
	tag := e.cache.GetTag(deviceID, tagName)
	if tag != nil {
		tag.Unit = &fieldMeta.Units
		tag.Description = &fieldMeta.Description
		e.cache.SetTag(deviceID, tagName, tag)
	}

	log.Printf("Enriched tag %s with catalog metadata", tagName)
	return nil
}
