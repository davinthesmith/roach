package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"weatherlink-sql-backfill/models"
)

// TagRepository handles database operations for tags
type TagRepository struct {
	pool *pgxpool.Pool
}

// NewTagRepository creates a new TagRepository
func NewTagRepository(pool *pgxpool.Pool) *TagRepository {
	return &TagRepository{pool: pool}
}

// LoadAll loads all tags from the database
func (r *TagRepository) LoadAll(ctx context.Context) ([]*models.Tag, error) {
	log.Println("Loading tags from database...")

	rows, err := r.pool.Query(ctx, `
		SELECT id, device_id, tag_name, data_type
		FROM tags
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query tags: %w", err)
	}
	defer rows.Close()

	var tags []*models.Tag
	for rows.Next() {
		var t models.Tag
		if err := rows.Scan(&t.ID, &t.DeviceID, &t.TagName, &t.DataType); err != nil {
			return nil, fmt.Errorf("failed to scan tag: %w", err)
		}
		tags = append(tags, &t)
	}

	log.Printf("Loaded %d tags from database", len(tags))
	return tags, nil
}

// CreateOrUpdate creates or updates a tag with metadata
func (r *TagRepository) CreateOrUpdate(ctx context.Context, deviceID int, tagName, dataType string, unit, description *string, metadata map[string]interface{}) (int, error) {
	var metadataJSON []byte
	var err error
	if metadata != nil && len(metadata) > 0 {
		metadataJSON, err = json.Marshal(metadata)
		if err != nil {
			log.Printf("Warning: Failed to marshal metadata for tag %s: %v", tagName, err)
			metadataJSON = nil
		}
	}

	var tagID int
	err = r.pool.QueryRow(ctx, `
		INSERT INTO tags (device_id, tag_name, data_type, unit, description, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (device_id, tag_name) DO UPDATE SET
			data_type = EXCLUDED.data_type,
			unit = COALESCE(EXCLUDED.unit, tags.unit),
			description = COALESCE(EXCLUDED.description, tags.description),
			metadata = COALESCE(EXCLUDED.metadata, tags.metadata),
			updated_at = NOW()
		RETURNING id
	`, deviceID, tagName, dataType, unit, description, metadataJSON).Scan(&tagID)

	return tagID, err
}

// EnrichWithCatalog backfills existing tags with metadata from catalog
func (r *TagRepository) EnrichWithCatalog(ctx context.Context, enrichFunc func(tagID, deviceID, sensorType int, dataStructureType *int, tagName string) error) error {
	log.Println("Enriching existing tags with catalog metadata...")

	// Query tags that need enrichment
	rows, err := r.pool.Query(ctx, `
		SELECT 
			t.id,
			t.device_id,
			t.tag_name,
			d.sensor_type,
			d.rt_data_structure_type
		FROM tags t
		JOIN devices d ON t.device_id = d.id
		WHERE 
			d.rt_data_structure_type IS NOT NULL
			AND (t.unit IS NULL OR t.description IS NULL OR t.metadata IS NULL)
	`)
	if err != nil {
		log.Printf("Warning: Failed to query tags for enrichment: %v", err)
		return err
	}
	defer rows.Close()

	enrichedCount := 0
	for rows.Next() {
		var tagID, deviceID, sensorType int
		var tagName string
		var dataStructureType *int

		if err := rows.Scan(&tagID, &deviceID, &tagName, &sensorType, &dataStructureType); err != nil {
			log.Printf("Warning: Failed to scan tag row: %v", err)
			continue
		}

		if err := enrichFunc(tagID, deviceID, sensorType, dataStructureType, tagName); err != nil {
			log.Printf("Warning: Failed to enrich tag %d: %v", tagID, err)
			continue
		}

		enrichedCount++
	}

	if enrichedCount > 0 {
		log.Printf("Enriched %d tags with catalog metadata", enrichedCount)
	}

	return nil
}

// Update updates tag metadata
func (r *TagRepository) Update(ctx context.Context, tagID int, unit, description string, metadata []byte) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE tags 
		SET 
			unit = COALESCE(unit, $1),
			description = COALESCE(description, $2),
			metadata = COALESCE(metadata, $3),
			updated_at = NOW()
		WHERE id = $4
	`, unit, description, metadata, tagID)
	return err
}

// Create creates a new tag with metadata
func (r *TagRepository) Create(ctx context.Context, deviceID int, tagName, dataType string, unit, description *string, metadata map[string]interface{}) (int, error) {
	return r.CreateOrUpdate(ctx, deviceID, tagName, dataType, unit, description, metadata)
}

// TagEnrichmentInfo holds information about a tag that needs enrichment
type TagEnrichmentInfo struct {
	TagID             int
	DeviceID          int
	TagName           string
	SensorType        int
	DataStructureType *int
}

// FindTagsNeedingEnrichment finds tags that need enrichment
func (r *TagRepository) FindTagsNeedingEnrichment(ctx context.Context) ([]*TagEnrichmentInfo, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT 
			t.id,
			t.device_id,
			t.tag_name,
			d.sensor_type,
			d.rt_data_structure_type
		FROM tags t
		JOIN devices d ON t.device_id = d.id
		WHERE 
			d.rt_data_structure_type IS NOT NULL
			AND (t.unit IS NULL OR t.description IS NULL OR t.metadata IS NULL)
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query tags for enrichment: %w", err)
	}
	defer rows.Close()

	var tags []*TagEnrichmentInfo
	for rows.Next() {
		var tag TagEnrichmentInfo
		if err := rows.Scan(&tag.TagID, &tag.DeviceID, &tag.TagName, &tag.SensorType, &tag.DataStructureType); err != nil {
			return nil, fmt.Errorf("failed to scan tag row: %w", err)
		}
		tags = append(tags, &tag)
	}

	return tags, nil
}

// Enrich enriches a tag with catalog metadata
func (r *TagRepository) Enrich(ctx context.Context, tagID int, unit, description string, metadata []byte) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE tags 
		SET 
			unit = COALESCE(unit, $1),
			description = COALESCE(description, $2),
			metadata = COALESCE(metadata, $3),
			updated_at = NOW()
		WHERE id = $4
	`, unit, description, metadata, tagID)
	return err
}
