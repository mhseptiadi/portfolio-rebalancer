package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"portfolio-rebalancer/internal/models"
	"portfolio-rebalancer/internal/storage"
)

type portfolioHandler struct {
	elasticStorage *storage.ElasticStorage
}

func NewPortfolioHandler(elasticStorage *storage.ElasticStorage) *portfolioHandler {
	return &portfolioHandler{elasticStorage: elasticStorage}
}

// SavePortfolio handles new portfolio creation requests (feel free to update the request parameter/model)
// Sample Request (POST /portfolio):
//
//	{
//	    "user_id": "1",
//	    "allocation": {"stocks": 60, "bonds": 30, "gold": 10}
//	}
func (h *portfolioHandler) SavePortfolio(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var p models.Portfolio
	err := json.NewDecoder(r.Body).Decode(&p)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	log.Println("HandlePortfolio==", p)

	// Check if user_id already exists
	portfolio, _ := h.elasticStorage.GetPortfolio(ctx, p.UserID)
	if portfolio != nil {
		http.Error(w, "User already exists", http.StatusBadRequest)
		return
	}

	// Save portfolio to Elasticsearch
	err = h.elasticStorage.SavePortfolio(ctx, p)
	if err != nil {
		http.Error(w, "Failed to save portfolio", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(p)
}

func (h *portfolioHandler) GetPortfolio(w http.ResponseWriter, r *http.Request) {
	log.Println("GetPortfolio==", r.URL.Query())
	ctx := r.Context()
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	p, err := h.elasticStorage.GetPortfolio(ctx, userID)
	if err != nil {
		if err.Error() == "user not found" {
			http.Error(w, "Portfolio not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to get portfolio", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(p)
}

func (h *portfolioHandler) ListPortfolios(w http.ResponseWriter, r *http.Request) {
	log.Println("ListPortfolios==", r.URL.Query())
	ctx := r.Context()
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	list, err := h.elasticStorage.ListPortfolios(ctx)
	if err != nil {
		http.Error(w, "Failed to list portfolios", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(list)
}

// HandleRebalance handles portfolio rebalance requests from 3rd party provider (feel free to update the request parameter/model)
// Sample Request (POST /rebalance):
//
//	{
//	    "user_id": "1",
//	    "new_allocation": {"stocks": 70, "bonds": 20, "gold": 10}
//	}
func (h *portfolioHandler) HandleRebalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.UpdatedPortfolio
	json.NewDecoder(r.Body).Decode(&req)

	log.Println("HandleRebalance==", req)

	// TODO: Add Logic here

	w.WriteHeader(http.StatusOK)
}
