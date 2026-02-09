package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"homeassistant-kafka/config"
	"homeassistant-kafka/ha"
	"homeassistant-kafka/kafka"
	"homeassistant-kafka/service"
)

func main() {
	log.Println("Starting ROACH Home Assistant Kafka Service...")

	cfg := config.Load()

	log.Printf("Configuration loaded:")
	log.Printf("  - HA URL: %s", cfg.HAURL)
	log.Printf("  - HA WS URL: %s", cfg.HAWSURL)
	log.Printf("  - Kafka Broker: %s", cfg.KafkaBroker)
	log.Printf("  - Poll Enabled: %t", cfg.PollEnabled)
	log.Printf("  - Poll Interval: %s", cfg.PollInterval)

	haClient := ha.NewClient(cfg)

	producer, err := kafka.NewProducer(cfg.KafkaBroker)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	svc := service.New(cfg, haClient, producer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	log.Println("Starting Home Assistant event stream...")
	if err := svc.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Service error: %v", err)
	}

	log.Println("Home Assistant Kafka service stopped")
}
