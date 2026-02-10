package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"ubiquiti-video-jpg/api"
	"ubiquiti-video-jpg/config"
	"ubiquiti-video-jpg/service"
)

func main() {
	cfg := config.Load()
	log.Printf("Starting ubiquiti-video-jpg service (log_level=%s)", cfg.LogLevel)
	log.Printf("Config: host=%s api_key=%s output_dir=%s retention=%v",
		cfg.UnifiHost, maskSecret(cfg.UnifiAPIKey), cfg.JPGOutputDir, cfg.Retention)

	apiClient := api.NewClient(cfg.UnifiAPIKey, cfg.UnifiHost, cfg.LogLevel)
	svc := service.New(cfg, apiClient)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, shutting down...", sig)
		cancel()
	}()

	if err := svc.Start(ctx); err != nil && err != context.Canceled {
		log.Printf("Service error: %v", err)
	}

	log.Println("Shutdown complete")
}

func maskSecret(s string) string {
	if len(s) <= 8 {
		return "***"
	}
	return s[:4] + "..." + s[len(s)-4:]
}
