package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"weatherlink-backfill-to-sql/config"
	"weatherlink-backfill-to-sql/service"
)

func main() {
	log.Println("Starting ROACH Kafka→DB Backfill Service...")

	// CLI flags
	topicsFlag := flag.String("topics", "", "Comma-separated list of topics to backfill (defaults to all weather topics)")
	startOffsetFlag := flag.Int64("start-offset", -2, "Start offset (-2=earliest, -1=latest, or specific offset)")
	endOffsetFlag := flag.Int64("end-offset", -1, "End offset (-1=latest, or specific offset)")
	workersFlag := flag.Int("workers", 0, "Number of worker threads (overrides WORKER_POOL_SIZE)")
	batchSizeFlag := flag.Int("batch-size", 0, "Batch size for database writes (overrides BATCH_SIZE)")
	metadataFlag := flag.Bool("metadata", false, "Include metadata topics in backfill")

	flag.Parse()

	// Load configuration from environment
	cfg := config.Load()

	// Override with CLI flags if provided
	if *topicsFlag != "" {
		topics := strings.Split(*topicsFlag, ",")
		for i := range topics {
			topics[i] = strings.TrimSpace(topics[i])
		}
		cfg.Topics = topics
	}
	if *startOffsetFlag != -2 {
		cfg.StartOffset = *startOffsetFlag
	}
	if *endOffsetFlag != -1 {
		cfg.EndOffset = *endOffsetFlag
	}
	if *workersFlag > 0 {
		cfg.WorkerPoolSize = *workersFlag
	}
	if *batchSizeFlag > 0 {
		cfg.BatchSize = *batchSizeFlag
	}
	if *metadataFlag {
		cfg.IncludeMetadata = true
	}

	// Validate configuration
	if cfg.PostgresDSN == "" {
		log.Fatal("POSTGRES_DSN is required")
	}

	log.Printf("Configuration loaded:")
	log.Printf("  - Kafka Broker: %s", cfg.KafkaBroker)
	log.Printf("  - Topics: %s", strings.Join(cfg.Topics, ", "))
	log.Printf("  - Start Offset: %s", formatOffset(cfg.StartOffset))
	log.Printf("  - End Offset: %s", formatOffset(cfg.EndOffset))
	log.Printf("  - Batch Size: %d", cfg.BatchSize)
	log.Printf("  - Worker Pool Size: %d", cfg.WorkerPoolSize)
	log.Printf("  - Batch Flush Interval: %dms", cfg.BatchFlushIntervalMs)
	log.Printf("  - DB Pool Max Conns: %d", cfg.DBPoolMaxConns)

	// Connect to database with pgxpool
	poolConfig, err := pgxpool.ParseConfig(cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("Failed to parse PostgreSQL DSN: %v", err)
	}

	poolConfig.MaxConns = int32(cfg.DBPoolMaxConns)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(context.Background()); err != nil {
		log.Fatalf("Failed to ping PostgreSQL: %v", err)
	}

	log.Println("Connected to PostgreSQL with connection pool")

	// Create backfill service
	backfillService, err := service.New(cfg, pool)
	if err != nil {
		log.Fatalf("Failed to create backfill service: %v", err)
	}
	defer backfillService.Close()

	// Setup signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	// Run backfill
	log.Println("Starting Kafka→DB backfill...")
	if err := backfillService.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Backfill error: %v", err)
	}

	log.Println("Kafka→DB backfill completed successfully")
}

func formatOffset(offset int64) string {
	switch offset {
	case -2:
		return "earliest"
	case -1:
		return "latest"
	default:
		return strconv.FormatInt(offset, 10)
	}
}
