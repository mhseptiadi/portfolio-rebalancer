package main

import (
	"log"
	"net/http"
	"portfolio-rebalancer/internal/config"
	"portfolio-rebalancer/internal/handlers"
	"portfolio-rebalancer/internal/kafka"
	"portfolio-rebalancer/internal/storage"
)

func main() {

	// Load config
	cfg := config.LoadConfig()
	log.Println("Config==", cfg)

	// Initializing elasticsearch if needed
	elasticStorage, err := storage.InitElastic(cfg.ElasticsearchURL)
	if err != nil {
		log.Fatalf("Failed to initialize Elasticsearch: %v", err)
	}
	kafka, err := kafka.InitKafka(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID, false)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka: %v", err)
	}

	portfolioHandler := handlers.NewPortfolioHandler(elasticStorage, kafka)

	mux := http.NewServeMux()
	mux.HandleFunc("/portfolio", portfolioHandler.SavePortfolio)
	mux.HandleFunc("/portfolio/id", portfolioHandler.GetPortfolio)
	mux.HandleFunc("/portfolios", portfolioHandler.ListPortfolios)

	mux.HandleFunc("/rebalance", portfolioHandler.HandleRebalance)

	mux.HandleFunc("/transactions", portfolioHandler.ListTransactions)

	log.Println("Server started at :8083")
	log.Fatal(http.ListenAndServe(":8083", mux))
}
