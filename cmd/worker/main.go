package main

import (
	"context"
	"log"
	"os/signal"
	"portfolio-rebalancer/internal/config"
	"portfolio-rebalancer/internal/handlers"
	"portfolio-rebalancer/internal/kafka"
	"portfolio-rebalancer/internal/storage"
	"sync"
	"syscall"
	"time"
)

func main() {

	// Load config
	cfg := config.LoadConfig()
	log.Println("Config==", cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	elasticStorage, err := storage.InitElastic(cfg.ElasticsearchURL)
	if err != nil {
		log.Fatalf("Failed to initialize Elasticsearch: %v", err)
	}

	var wg sync.WaitGroup
	kafkaClients := make([]*kafka.Kafka, cfg.NumWorkers)

	for i := 0; i < cfg.NumWorkers; i++ {
		kafkaClients[i], err = kafka.InitKafka(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
		if err != nil {
			log.Printf("Failed to initialize Kafka: %v", err)
			stop()
			break
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			log.Printf("Starting worker %d", i)
			portfolioHandler := handlers.NewPortfolioHandler(elasticStorage, kafkaClients[i])
			err := kafkaClients[i].ConsumeMessage(ctx, portfolioHandler.HandleRebalanceMessage)
			if err != nil {
				log.Printf("Worker failed: %v", err)
				stop()
				return
			}
		}(i)
	}

	<-ctx.Done()

	workersDone := make(chan struct{})

	go func() {
		wg.Wait()
		close(workersDone)
	}()

	select {
	case <-workersDone:
		log.Println("All Kafka workers stopped cleanly.")
	case <-time.After(10 * time.Second):
		log.Println("Timeout reached. Forcing shutdown.")
	}

	for i, client := range kafkaClients {
		if client != nil {
			if err := client.Close(); err != nil {
				log.Printf("Failed to close Kafka client %d: %v", i, err)
			}
		}
	}
}
