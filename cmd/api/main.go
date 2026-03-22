package main

import (
	"log"
	"net/http"
	"portfolio-rebalancer/internal/config"
	"portfolio-rebalancer/internal/handlers"
	"portfolio-rebalancer/internal/storage"
)

func main() {

	// Load config
	cfg := config.LoadConfig()

	// Initializing elasticsearch if needed
	elasticStorage, err := storage.InitElastic(cfg.ElasticsearchURL)
	if err != nil {
		log.Fatalf("Failed to initialize Elasticsearch: %v", err)
	}

	portfolioHandler := handlers.NewPortfolioHandler(elasticStorage)

	mux := http.NewServeMux()
	mux.HandleFunc("/portfolio", portfolioHandler.SavePortfolio)
	mux.HandleFunc("/portfolio/id", portfolioHandler.GetPortfolio)
	mux.HandleFunc("/portfolios", portfolioHandler.ListPortfolios)

	mux.HandleFunc("/rebalance", portfolioHandler.HandleRebalance)

	log.Println("Server started at :8083")
	log.Fatal(http.ListenAndServe(":8083", mux))
}
