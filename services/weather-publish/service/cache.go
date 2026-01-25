package service

import (
	"context"
	"fmt"
	"log"
	"time"
)

// rehydrateCache loads recent timestamps from the database to populate the cache
func (s *Service) rehydrateCache(ctx context.Context) error {
	if s.db == nil {
		log.Println("PostgreSQL not configured, skipping cache rehydration")
		return nil
	}

	log.Println("Rehydrating timestamp cache from PostgreSQL...")

	// Query for max timestamps per device from last 24 hours
	query := `
		SELECT device_id, MAX(timestamp) as last_timestamp
		FROM (
			SELECT device_id, timestamp FROM records_numeric WHERE timestamp > $1
			UNION ALL
			SELECT device_id, timestamp FROM records_text WHERE timestamp > $1
			UNION ALL
			SELECT device_id, timestamp FROM records_null WHERE timestamp > $1
		) combined
		GROUP BY device_id
	`

	// Calculate 24 hours ago in Unix timestamp
	oneDayAgo := time.Now().Add(-24 * time.Hour).Unix()

	rows, err := s.db.QueryContext(ctx, query, oneDayAgo)
	if err != nil {
		// If tables don't exist yet, that's okay - cache starts empty
		log.Printf("Warning: Could not rehydrate cache (tables may not exist yet): %v", err)
		return nil
	}
	defer rows.Close()

	s.cacheMutex.Lock()
	defer s.cacheMutex.Unlock()

	count := 0
	for rows.Next() {
		var deviceID int
		var lastTimestamp int64
		if err := rows.Scan(&deviceID, &lastTimestamp); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		s.timestampCache[deviceID] = lastTimestamp
		count++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	log.Printf("Rehydrated cache with %d device timestamps", count)
	return nil
}
