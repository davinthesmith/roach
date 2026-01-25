package repository

import (
	"context"
	"database/sql"
	"fmt"

	"weather-sql/models"
)

// RecordRepository handles database operations for records
type RecordRepository struct {
	db *sql.DB
}

// NewRecordRepository creates a new RecordRepository
func NewRecordRepository(db *sql.DB) *RecordRepository {
	return &RecordRepository{db: db}
}

// InsertNumeric inserts a numeric record
func (r *RecordRepository) InsertNumeric(ctx context.Context, tagID int, value float64, timestamp int64) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO records_numeric (tag_id, value, ts)
		VALUES ($1, $2, $3)
		ON CONFLICT (tag_id, ts) DO NOTHING
	`, tagID, value, timestamp)
	return err
}

// InsertText inserts a text record
func (r *RecordRepository) InsertText(ctx context.Context, tagID int, value string, timestamp int64) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO records_text (tag_id, value, ts)
		VALUES ($1, $2, $3)
		ON CONFLICT (tag_id, ts) DO NOTHING
	`, tagID, value, timestamp)
	return err
}

// InsertNull inserts a null record
func (r *RecordRepository) InsertNull(ctx context.Context, tagID int, timestamp int64) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO records_null (tag_id, ts)
		VALUES ($1, $2)
		ON CONFLICT (tag_id, ts) DO NOTHING
	`, tagID, timestamp)
	return err
}

// Insert inserts a record based on the tag's data type
func (r *RecordRepository) Insert(ctx context.Context, tag *models.Tag, value interface{}, timestamp int64) error {
	if value == nil {
		return r.InsertNull(ctx, tag.ID, timestamp)
	}

	switch tag.DataType {
	case "numeric", "float", "integer":
		var numValue float64
		switch v := value.(type) {
		case float64:
			numValue = v
		case float32:
			numValue = float64(v)
		case int:
			numValue = float64(v)
		case int64:
			numValue = float64(v)
		default:
			return fmt.Errorf("unsupported numeric type for value: %T", value)
		}
		return r.InsertNumeric(ctx, tag.ID, numValue, timestamp)

	case "text", "string":
		strValue, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected string value, got %T", value)
		}
		return r.InsertText(ctx, tag.ID, strValue, timestamp)

	default:
		return fmt.Errorf("unknown data type: %s", tag.DataType)
	}
}
