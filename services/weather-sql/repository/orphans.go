package repository

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

// OrphanRepository handles database operations for orphaned messages
type OrphanRepository struct {
	db *sql.DB
}

// NewOrphanRepository creates a new OrphanRepository
func NewOrphanRepository(db *sql.DB) *OrphanRepository {
	return &OrphanRepository{db: db}
}

// Save saves an orphaned message
func (r *OrphanRepository) Save(ctx context.Context, msg kafka.Message, lsid int, timestamp int64, tagName, reason string) error {
	// Marshal headers
	headersMap := make(map[string]string)
	for _, h := range msg.Headers {
		headersMap[h.Key] = string(h.Value)
	}
	headersJSON, _ := json.Marshal(headersMap)

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO orphaned_messages (topic, partition, offset, lsid, timestamp, tag_name, reason, message_headers, message_body)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (topic, partition, offset) DO UPDATE SET
			lsid = EXCLUDED.lsid,
			timestamp = EXCLUDED.timestamp,
			tag_name = EXCLUDED.tag_name,
			reason = EXCLUDED.reason,
			message_headers = EXCLUDED.message_headers,
			message_body = EXCLUDED.message_body,
			created_at = NOW()
	`, msg.Topic, msg.Partition, msg.Offset, lsid, timestamp, tagName, reason, headersJSON, msg.Value)

	return err
}
