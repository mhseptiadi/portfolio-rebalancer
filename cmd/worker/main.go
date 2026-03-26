package main

import (
	"context"
	"log"
	"os/signal"
	"portfolio-rebalancer/internal/config"
	"portfolio-rebalancer/internal/handlers"
	"portfolio-rebalancer/internal/kafka"
	"portfolio-rebalancer/internal/storage"
	"syscall"
	"time"
)

func main() {

	// Load config
	cfg := config.LoadConfig()
	// log.Println("Config==", cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Initializing elasticsearch if needed
	elasticStorage, err := storage.InitElastic(cfg.ElasticsearchURL)
	if err != nil {
		log.Fatalf("Failed to initialize Elasticsearch: %v", err)
	}
	kafkaClient, err := kafka.InitKafka(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka: %v", err)
	}

	portfolioHandler := handlers.NewPortfolioHandler(elasticStorage, kafkaClient)

	// Block until the consumer loop exits (normally when ctx is cancelled).
	done := make(chan error, 1)
	go func() {
		done <- kafkaClient.ConsumeMessage(ctx, portfolioHandler.HandleRebalanceMessage)
	}()

	select {
	case err := <-done:
		if err != nil {
			log.Printf("kafka consumer stopped: %v", err)
		}
	case <-ctx.Done():
		// Give the loop a short time to shut down cleanly.
		if errors := kafkaClient.Close(); len(errors) > 0 {
			for _, err := range errors {
				log.Printf("failed to close kafka client: %v", err)
			}
		}

		select {
		case err := <-done:
			if err != nil {
				log.Printf("kafka consumer stopped with error: %v", err)
			}
			log.Println("kafka consumer stopped")
		case <-time.After(10 * time.Second):
			log.Println("shutdown timed out; forcing exit")
		}

	}
}
