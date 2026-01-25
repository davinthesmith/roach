package repository

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"weatherlink-sql/models"
)

// CatalogRepository handles database operations for sensor catalog
type CatalogRepository struct {
	pool *pgxpool.Pool
}

// NewCatalogRepository creates a new CatalogRepository
func NewCatalogRepository(pool *pgxpool.Pool) *CatalogRepository {
	return &CatalogRepository{pool: pool}
}

// LoadAll loads all catalog entries from the database
func (r *CatalogRepository) LoadAll(ctx context.Context) ([]*models.FieldMetadata, error) {
	log.Println("Loading catalog from database...")

	rows, err := r.pool.Query(ctx, `
		SELECT sensor_type, data_structure_type, field_name, field_type, units, description
		FROM sensor_catalog
	`)
	if err != nil {
		// Table might not exist yet
		log.Printf("Warning: Could not load catalog (table may not exist yet): %v", err)
		return nil, nil
	}
	defer rows.Close()

	var entries []*models.FieldMetadata
	for rows.Next() {
		var sensorType int
		var dataStructureType, fieldName, fieldType, units, description string

		if err := rows.Scan(&sensorType, &dataStructureType, &fieldName, &fieldType, &units, &description); err != nil {
			log.Printf("Warning: Failed to scan catalog row: %v", err)
			continue
		}

		metadata := &models.FieldMetadata{
			FieldName:         fieldName,
			FieldType:         fieldType,
			Units:             units,
			Description:       description,
			DataStructureType: dataStructureType,
			SensorType:        sensorType,
			RawMetadata:       make(map[string]interface{}), // Empty map for DB-loaded entries
		}

		entries = append(entries, metadata)
	}

	log.Printf("Loaded %d catalog entries from database", len(entries))
	return entries, nil
}

// Upsert creates or updates a catalog entry
func (r *CatalogRepository) Upsert(ctx context.Context, sensorType int, dsType, fieldName, fieldType, units, description string) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO sensor_catalog (sensor_type, data_structure_type, field_name, field_type, units, description, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW())
		ON CONFLICT (sensor_type, data_structure_type, field_name) DO UPDATE SET
			field_type = EXCLUDED.field_type,
			units = EXCLUDED.units,
			description = EXCLUDED.description,
			updated_at = NOW()
	`, sensorType, dsType, fieldName, fieldType, units, description)

	if err != nil {
		return fmt.Errorf("failed to upsert catalog entry for %d/%s/%s: %w", sensorType, dsType, fieldName, err)
	}

	return nil
}
