package main

import (
	"context"
	"log"
	"portfolio-rebalancer/internal/config"
	"portfolio-rebalancer/internal/handlers"
	"portfolio-rebalancer/internal/kafka"
	"portfolio-rebalancer/internal/storage"
	"time"
)

func main() {

	// Load config
	cfg := config.LoadConfig()
	log.Println("Config==", cfg)

	ctx := context.Background()

	// Initializing elasticsearch if needed
	elasticStorage, err := storage.InitElastic(cfg.ElasticsearchURL)
	if err != nil {
		log.Fatalf("Failed to initialize Elasticsearch: %v", err)
	}
	kafka, err := kafka.InitKafka(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka: %v", err)
	}

	portfolioHandler := handlers.NewPortfolioHandler(elasticStorage, kafka)
	log.Println("portfolioHandler== 123", portfolioHandler)

	// Block until the consumer loop exits (normally when ctx is cancelled).
	done := make(chan error, 1)
	go func() {
		done <- kafka.ConsumeMessage(ctx, portfolioHandler.HandleRebalanceMessage)
	}()

	select {
	case err := <-done:
		if err != nil {
			log.Printf("kafka consumer stopped: %v", err)
		}
	case <-ctx.Done():
		// Give the loop a short time to shut down cleanly.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = shutdownCtx
	}
}
