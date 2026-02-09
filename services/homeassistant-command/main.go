package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"homeassistant-command/config"
	"homeassistant-command/ha"
	"homeassistant-command/kafka"
	"homeassistant-command/service"
)

func main() {
	log.Println("Starting ROACH Home Assistant Command Service...")

	cfg := config.Load()

	log.Printf("Configuration loaded:")
	log.Printf("  - HA URL: %s", cfg.HAURL)
	log.Printf("  - HA WS URL: %s", cfg.HAWSURL)
	log.Printf("  - Kafka Broker: %s", cfg.KafkaBroker)
	log.Printf("  - Kafka Topic: %s", cfg.KafkaTopic)
	log.Printf("  - Kafka Consumer Group: %s", cfg.KafkaConsumerGroup)

	haClient := ha.NewClient(cfg)
	reader := kafka.NewCommandReader(cfg)

	svc := service.New(cfg, haClient, reader)
	defer svc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	log.Println("Starting command consumer...")
	if err := svc.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Service error: %v", err)
	}

	log.Println("Home Assistant Command service stopped")
}
