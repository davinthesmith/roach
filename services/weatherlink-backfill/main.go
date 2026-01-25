package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"weatherlink-backfill/config"
	"weatherlink-backfill/service"

	"github.com/roach/weatherlink-lib/api"
	"github.com/roach/weatherlink-lib/kafka"
)

// parseTimestamp parses a timestamp string that can be either:
// - Unix timestamp (e.g., "1768780863")
// - Datetime string (e.g., "2026-01-11 18:20:47")
func parseTimestamp(s string) (int64, error) {
	// Try parsing as Unix timestamp first
	if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
		return ts, nil
	}
	
	// Try parsing as datetime string
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
	}
	
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Unix(), nil
		}
	}
	
	return 0, nil
}

func main() {
	// CLI flags
	startStr := flag.String("start", "", "Start timestamp (Unix seconds or datetime like '2026-01-11 18:20:47', required)")
	endStr := flag.String("end", "", "End timestamp (Unix seconds or datetime like '2026-01-11 18:20:47', defaults to now)")
	requestsPerSecond := flag.Int("requests-per-second", 8, "API requests per second (default: 8)")
	parallelWorkers := flag.Int("workers", 4, "Number of parallel workers (default: 4)")
	
	flag.Parse()
	
	// Validate required flags
	if *startStr == "" {
		log.Fatal("--start timestamp is required")
	}
	
	// Parse start timestamp
	startTs, err := parseTimestamp(*startStr)
	if err != nil || startTs == 0 {
		log.Fatalf("Invalid --start timestamp: %s (use Unix seconds or format like '2026-01-11 18:20:47')", *startStr)
	}
	
	// Parse or default end timestamp
	var endTs int64
	if *endStr == "" {
		endTs = time.Now().Unix()
	} else {
		endTs, err = parseTimestamp(*endStr)
		if err != nil || endTs == 0 {
			log.Fatalf("Invalid --end timestamp: %s (use Unix seconds or format like '2026-01-11 18:20:47')", *endStr)
		}
	}
	
	// Validate time range
	if startTs >= endTs {
		log.Fatal("Start timestamp must be before end timestamp")
	}
	
	log.Println("Starting ROACH WeatherLink Backfill Service...")
	log.Printf("Time range: %s to %s (%d seconds)",
		time.Unix(startTs, 0).Format("2006-01-02 15:04:05"),
		time.Unix(endTs, 0).Format("2006-01-02 15:04:05"),
		endTs-startTs)
	log.Printf("Rate limit: %d requests/second", *requestsPerSecond)
	log.Printf("Parallel workers: %d", *parallelWorkers)
	
	// Load configuration from environment
	cfg := config.Load()
	cfg.RequestsPerSecond = *requestsPerSecond // Override with CLI flag
	cfg.ParallelWorkers = *parallelWorkers     // Override with CLI flag
	
	// Validate configuration
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}
	
	log.Printf("Configuration loaded:")
	log.Printf("  - Station ID: %s", cfg.WeatherLinkStationID)
	log.Printf("  - Kafka Broker: %s", cfg.KafkaBroker)
	log.Printf("  - Requests/second: %d", cfg.RequestsPerSecond)
	log.Printf("  - Parallel workers: %d", cfg.ParallelWorkers)
	
	// Create API client
	apiClient := api.NewClient(cfg.WeatherLinkAPIKey, cfg.WeatherLinkAPISecret, cfg.WeatherLinkStationID)
	
	// Create Kafka producer
	producer, err := kafka.NewProducer(cfg.KafkaBroker)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()
	
	// Create backfill service
	svc := service.New(apiClient, producer, cfg.RequestsPerSecond, cfg.ParallelWorkers)
	
	// Setup signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	go func() {
		<-sigChan
		log.Println("Received shutdown signal, stopping backfill...")
		cancel()
	}()
	
	// Run backfill
	log.Println("Starting backfill...")
	if err := svc.Backfill(ctx, startTs, endTs); err != nil {
		if err == context.Canceled {
			log.Println("Backfill canceled by user")
		} else {
			log.Fatalf("Backfill error: %v", err)
		}
	}
	
	log.Println("Backfill service completed successfully")
}
