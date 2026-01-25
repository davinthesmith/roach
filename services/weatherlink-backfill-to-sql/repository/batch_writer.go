package repository

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Record types for batching
type numericRecord struct {
	TagID     int
	Value     float64
	Timestamp int64
}

type textRecord struct {
	TagID     int
	Value     string
	Timestamp int64
}

type nullRecord struct {
	TagID     int
	Timestamp int64
}

// BatchWriter handles batched writes to the database using COPY protocol
type BatchWriter struct {
	pool              *pgxpool.Pool
	numericBatch      []numericRecord
	textBatch         []textRecord
	nullBatch         []nullRecord
	mutex             sync.Mutex
	flushMutex        sync.Mutex // Serializes all flush operations to prevent deadlocks
	flushSize         int
	flushInterval     time.Duration
	lastFlush         time.Time
	stopChan          chan struct{}
	wg                sync.WaitGroup
	
	// Metrics
	totalNumericInserts int64
	totalTextInserts    int64
	totalNullInserts    int64
	totalFlushes        int64
}

// NewBatchWriter creates a new BatchWriter
func NewBatchWriter(pool *pgxpool.Pool, flushSize int, flushInterval time.Duration) *BatchWriter {
	bw := &BatchWriter{
		pool:          pool,
		numericBatch:  make([]numericRecord, 0, flushSize),
		textBatch:     make([]textRecord, 0, flushSize),
		nullBatch:     make([]nullRecord, 0, flushSize),
		flushSize:     flushSize,
		flushInterval: flushInterval,
		lastFlush:     time.Now(),
		stopChan:      make(chan struct{}),
	}

	// Start background flusher
	bw.wg.Add(1)
	go bw.backgroundFlusher()

	return bw
}

// AddNumeric adds a numeric record to the batch
func (bw *BatchWriter) AddNumeric(tagID int, value float64, timestamp int64) error {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()

	bw.numericBatch = append(bw.numericBatch, numericRecord{
		TagID:     tagID,
		Value:     value,
		Timestamp: timestamp,
	})

	// Check if we should flush
	if len(bw.numericBatch) >= bw.flushSize {
		return bw.flushNumericLocked(context.Background())
	}

	return nil
}

// AddText adds a text record to the batch
func (bw *BatchWriter) AddText(tagID int, value string, timestamp int64) error {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()

	bw.textBatch = append(bw.textBatch, textRecord{
		TagID:     tagID,
		Value:     value,
		Timestamp: timestamp,
	})

	// Check if we should flush
	if len(bw.textBatch) >= bw.flushSize {
		return bw.flushTextLocked(context.Background())
	}

	return nil
}

// AddNull adds a null record to the batch
func (bw *BatchWriter) AddNull(tagID int, timestamp int64) error {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()

	bw.nullBatch = append(bw.nullBatch, nullRecord{
		TagID:     tagID,
		Timestamp: timestamp,
	})

	// Check if we should flush
	if len(bw.nullBatch) >= bw.flushSize {
		return bw.flushNullLocked(context.Background())
	}

	return nil
}

// backgroundFlusher periodically flushes batches based on time interval
func (bw *BatchWriter) backgroundFlusher() {
	defer bw.wg.Done()
	ticker := time.NewTicker(bw.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bw.FlushAll(context.Background())
		case <-bw.stopChan:
			return
		}
	}
}

// FlushAll flushes all batches
func (bw *BatchWriter) FlushAll(ctx context.Context) error {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()

	var errs []error

	if err := bw.flushNumericLocked(ctx); err != nil {
		errs = append(errs, fmt.Errorf("numeric flush: %w", err))
	}

	if err := bw.flushTextLocked(ctx); err != nil {
		errs = append(errs, fmt.Errorf("text flush: %w", err))
	}

	if err := bw.flushNullLocked(ctx); err != nil {
		errs = append(errs, fmt.Errorf("null flush: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("flush errors: %v", errs)
	}

	return nil
}

// flushNumericLocked flushes numeric batch (must be called with mutex held)
func (bw *BatchWriter) flushNumericLocked(ctx context.Context) error {
	if len(bw.numericBatch) == 0 {
		return nil
	}

	batch := bw.numericBatch
	bw.numericBatch = make([]numericRecord, 0, bw.flushSize)

	// Release batch mutex during I/O, but hold flush mutex to serialize DB operations
	bw.mutex.Unlock()
	bw.flushMutex.Lock()
	err := bw.copyNumericRecords(ctx, batch)
	bw.flushMutex.Unlock()
	bw.mutex.Lock()

	if err != nil {
		log.Printf("Error flushing %d numeric records: %v", len(batch), err)
		return err
	}

	bw.totalNumericInserts += int64(len(batch))
	bw.totalFlushes++
	bw.lastFlush = time.Now()

	return nil
}

// flushTextLocked flushes text batch (must be called with mutex held)
func (bw *BatchWriter) flushTextLocked(ctx context.Context) error {
	if len(bw.textBatch) == 0 {
		return nil
	}

	batch := bw.textBatch
	bw.textBatch = make([]textRecord, 0, bw.flushSize)

	// Release batch mutex during I/O, but hold flush mutex to serialize DB operations
	bw.mutex.Unlock()
	bw.flushMutex.Lock()
	err := bw.copyTextRecords(ctx, batch)
	bw.flushMutex.Unlock()
	bw.mutex.Lock()

	if err != nil {
		log.Printf("Error flushing %d text records: %v", len(batch), err)
		return err
	}

	bw.totalTextInserts += int64(len(batch))
	bw.totalFlushes++
	bw.lastFlush = time.Now()

	return nil
}

// flushNullLocked flushes null batch (must be called with mutex held)
func (bw *BatchWriter) flushNullLocked(ctx context.Context) error {
	if len(bw.nullBatch) == 0 {
		return nil
	}

	batch := bw.nullBatch
	bw.nullBatch = make([]nullRecord, 0, bw.flushSize)

	// Release batch mutex during I/O, but hold flush mutex to serialize DB operations
	bw.mutex.Unlock()
	bw.flushMutex.Lock()
	err := bw.copyNullRecords(ctx, batch)
	bw.flushMutex.Unlock()
	bw.mutex.Lock()

	if err != nil {
		log.Printf("Error flushing %d null records: %v", len(batch), err)
		return err
	}

	bw.totalNullInserts += int64(len(batch))
	bw.totalFlushes++
	bw.lastFlush = time.Now()

	return nil
}

// copyNumericRecords uses COPY protocol to bulk insert numeric records
func (bw *BatchWriter) copyNumericRecords(ctx context.Context, records []numericRecord) error {
	// Use a transaction to ensure all operations use the same connection
	tx, err := bw.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create a temporary table for deduplication
	_, err = tx.Exec(ctx, `
		CREATE TEMP TABLE temp_numeric (
			tag_id INTEGER,
			value NUMERIC,
			ts BIGINT
		) ON COMMIT DROP
	`)
	if err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	// Use COPY to load data into temp table
	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"temp_numeric"},
		[]string{"tag_id", "value", "ts"},
		pgx.CopyFromSlice(len(records), func(i int) ([]interface{}, error) {
			return []interface{}{records[i].TagID, records[i].Value, records[i].Timestamp}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to copy records: %w", err)
	}

	// Insert from temp table with deduplication
	_, err = tx.Exec(ctx, `
		INSERT INTO records_numeric (tag_id, value, ts)
		SELECT tag_id, value, ts FROM temp_numeric
		ON CONFLICT (tag_id, ts) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("failed to insert from temp table: %w", err)
	}

	return tx.Commit(ctx)
}

// copyTextRecords uses COPY protocol to bulk insert text records
func (bw *BatchWriter) copyTextRecords(ctx context.Context, records []textRecord) error {
	// Use a transaction to ensure all operations use the same connection
	tx, err := bw.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create a temporary table for deduplication
	_, err = tx.Exec(ctx, `
		CREATE TEMP TABLE temp_text (
			tag_id INTEGER,
			value TEXT,
			ts BIGINT
		) ON COMMIT DROP
	`)
	if err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	// Use COPY to load data into temp table
	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"temp_text"},
		[]string{"tag_id", "value", "ts"},
		pgx.CopyFromSlice(len(records), func(i int) ([]interface{}, error) {
			return []interface{}{records[i].TagID, records[i].Value, records[i].Timestamp}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to copy records: %w", err)
	}

	// Insert from temp table with deduplication
	_, err = tx.Exec(ctx, `
		INSERT INTO records_text (tag_id, value, ts)
		SELECT tag_id, value, ts FROM temp_text
		ON CONFLICT (tag_id, ts) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("failed to insert from temp table: %w", err)
	}

	return tx.Commit(ctx)
}

// copyNullRecords uses COPY protocol to bulk insert null records
func (bw *BatchWriter) copyNullRecords(ctx context.Context, records []nullRecord) error {
	// Use a transaction to ensure all operations use the same connection
	tx, err := bw.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create a temporary table for deduplication
	_, err = tx.Exec(ctx, `
		CREATE TEMP TABLE temp_null (
			tag_id INTEGER,
			ts BIGINT
		) ON COMMIT DROP
	`)
	if err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	// Use COPY to load data into temp table
	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"temp_null"},
		[]string{"tag_id", "ts"},
		pgx.CopyFromSlice(len(records), func(i int) ([]interface{}, error) {
			return []interface{}{records[i].TagID, records[i].Timestamp}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to copy records: %w", err)
	}

	// Insert from temp table with deduplication
	_, err = tx.Exec(ctx, `
		INSERT INTO records_null (tag_id, ts)
		SELECT tag_id, ts FROM temp_null
		ON CONFLICT (tag_id, ts) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("failed to insert from temp table: %w", err)
	}

	return tx.Commit(ctx)
}

// Close stops the background flusher and flushes remaining records
func (bw *BatchWriter) Close(ctx context.Context) error {
	// Stop background flusher
	close(bw.stopChan)
	bw.wg.Wait()

	// Flush remaining records
	return bw.FlushAll(ctx)
}

// GetMetrics returns current metrics
func (bw *BatchWriter) GetMetrics() (numericInserts, textInserts, nullInserts, flushes int64) {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()
	return bw.totalNumericInserts, bw.totalTextInserts, bw.totalNullInserts, bw.totalFlushes
}

// GetBatchSizes returns current batch sizes
func (bw *BatchWriter) GetBatchSizes() (numeric, text, null int) {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()
	return len(bw.numericBatch), len(bw.textBatch), len(bw.nullBatch)
}
