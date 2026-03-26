package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"portfolio-rebalancer/internal/kafka"
	"portfolio-rebalancer/internal/models"
	"portfolio-rebalancer/internal/services"
	"portfolio-rebalancer/internal/storage"
	"time"
)

// PortfolioHandler serves the portfolio HTTP API and Kafka rebalance consumer callbacks.
type PortfolioHandler struct {
	store storage.Store
	kafka kafka.MessagePublisher
}

func NewPortfolioHandler(store storage.Store, pub kafka.MessagePublisher) *PortfolioHandler {
	return &PortfolioHandler{store: store, kafka: pub}
}

// SavePortfolio handles new portfolio creation requests (feel free to update the request parameter/model)
// Sample Request (POST /portfolio):
//
//	{
//	    "user_id": "1",
//	    "allocation": {"stocks": 60, "bonds": 30, "gold": 10}
//	}
func (h *PortfolioHandler) SavePortfolio(w http.ResponseWriter, r *http.Request) {
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

	p.CreatedAt = time.Now()
	p.UpdatedAt = time.Now()

	if err := models.ValidateAllocation(p.Allocation); err != nil {
		http.Error(w, "Invalid allocation", http.StatusBadRequest)
		return
	}

	// Check if user_id already exists
	portfolio, _ := h.store.GetPortfolio(ctx, p.UserID)
	if portfolio != nil {
		http.Error(w, "User already exists", http.StatusBadRequest)
		return
	}

	// Save portfolio to Elasticsearch
	err = h.store.SavePortfolio(ctx, p)
	if err != nil {
		http.Error(w, "Failed to save portfolio", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(p)
}

func (h *PortfolioHandler) GetPortfolio(w http.ResponseWriter, r *http.Request) {
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

	p, err := h.store.GetPortfolio(ctx, userID)
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

func (h *PortfolioHandler) ListPortfolios(w http.ResponseWriter, r *http.Request) {
	log.Println("ListPortfolios==", r.URL.Query())
	ctx := r.Context()
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	list, err := h.store.ListPortfolios(ctx)
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
func (h *PortfolioHandler) HandleRebalance(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.UpdatedPortfolio
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.UserID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	if err := models.ValidateAllocation(req.NewAllocation); err != nil {
		http.Error(w, "Invalid allocation", http.StatusBadRequest)
		return
	}

	portfolio, err := h.store.GetPortfolio(ctx, req.UserID)
	if err != nil {
		if err.Error() == "user not found" {
			http.Error(w, "Portfolio not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to get portfolio", http.StatusInternalServerError)
		return
	}
	if portfolio == nil {
		http.Error(w, "Portfolio not found", http.StatusNotFound)
		return
	}

	if h.kafka == nil {
		http.Error(w, "kafka is not configured", http.StatusInternalServerError)
		return
	}

	payload, err := json.Marshal(req)
	if err != nil {
		http.Error(w, "Failed to marshal request", http.StatusInternalServerError)
		return
	}

	if err := h.kafka.PublishMessage(r.Context(), payload); err != nil {
		log.Println("failed to publish rebalance request", err)
		http.Error(w, "failed to enqueue rebalance", http.StatusInternalServerError)
		return
	}

	log.Println("HandleRebalance==", string(payload))

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode("Rebalance request published")
}

func (h *PortfolioHandler) ListTransactions(w http.ResponseWriter, r *http.Request) {
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

	list, err := h.store.ListTransactions(ctx, userID)
	if err != nil {
		http.Error(w, "Failed to list transactions", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(list)
}

func (h *PortfolioHandler) HandleRebalanceMessage(ctx context.Context, msg []byte) error {
	var req models.UpdatedPortfolio
	err := json.Unmarshal(msg, &req)
	if err != nil {
		return err
	}

	if req.UserID == "" {
		log.Println("user_id is required")
		return nil
	}

	if err := models.ValidateAllocation(req.NewAllocation); err != nil {
		log.Println("invalid allocation")
		return nil
	}

	portfolio, err := h.store.GetPortfolio(ctx, req.UserID)
	if err != nil {
		log.Println("failed to get portfolio")
		return nil
	}
	if portfolio == nil {
		log.Println("portfolio not found")
		return nil
	}

	if portfolio.Allocation.Stocks == req.NewAllocation.Stocks && portfolio.Allocation.Bonds == req.NewAllocation.Bonds && portfolio.Allocation.Gold == req.NewAllocation.Gold {
		log.Println("new allocation is the same as the current allocation")
		return nil
	}

	targetPortfolio := models.Portfolio{
		UserID:     req.UserID,
		Allocation: req.NewAllocation,
		CreatedAt:  portfolio.CreatedAt,
		UpdatedAt:  time.Now(),
	}

	rebalanceTransactions, err := services.CalculateRebalance(*portfolio, targetPortfolio)
	if err != nil {
		log.Println("failed to calculate rebalance")
		return fmt.Errorf("failed to calculate rebalance")
	}

	if err := h.store.SaveRebalance(ctx, targetPortfolio, rebalanceTransactions); err != nil {
		log.Println("failed to save rebalance", err)
		return err
	}

	log.Println("HandleRebalanceMessage completed")
	return nil
}
