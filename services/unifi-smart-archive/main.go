package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"unifi-smart-archive/config"
	"unifi-smart-archive/kafka"
	"unifi-smart-archive/service"
)

func main() {
	log.Println("Starting unifi-smart-archive...")

	cfg := config.Load()

	log.Printf("Config: broker=%s topic=%s source=%s archive=%s retention=%dd event_end_timeout=%v",
		cfg.KafkaBroker, cfg.KafkaTopic, cfg.SourceDir, cfg.ArchiveDir,
		cfg.ArchiveRetentionDays, cfg.EventEndTimeout)

	reader := kafka.NewReader(cfg)
	svc := service.New(cfg, reader)
	defer svc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutdown signal received")
		cancel()
	}()

	if err := svc.Start(ctx); err != nil && err != context.Canceled {
		log.Printf("Service exited: %v", err)
		os.Exit(1)
	}
	log.Println("unifi-smart-archive stopped")
}
