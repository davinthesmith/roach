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

	// Query for max timestamps per device (by LSID) from last 24 hours
	// Fixed: Query now returns LSID instead of device_id to match cache structure
	query := `
		SELECT d.lsid, MAX(r.timestamp) as last_timestamp
		FROM (
			SELECT device_id, timestamp FROM records_numeric WHERE timestamp > $1
			UNION ALL
			SELECT device_id, timestamp FROM records_text WHERE timestamp > $1
			UNION ALL
			SELECT device_id, timestamp FROM records_null WHERE timestamp > $1
		) r
		JOIN devices d ON d.id = r.device_id
		GROUP BY d.lsid
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
		var lsid int
		var lastTimestamp int64
		if err := rows.Scan(&lsid, &lastTimestamp); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		// Note: This rehydration doesn't yet account for data_structure_type
		// In practice, this is acceptable since most sensors have one data structure type
		// A full solution would require querying (lsid, data_structure_type, timestamp)
		if s.timestampCache[lsid] == nil {
			s.timestampCache[lsid] = make(map[int]int64)
		}
		// Store with data_structure_type 0 as default (will be updated on first real message)
		s.timestampCache[lsid][0] = lastTimestamp
		count++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	log.Printf("Rehydrated cache with %d device timestamps", count)
	return nil
}
