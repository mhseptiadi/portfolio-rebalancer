package storage

import (
	"context"

	"portfolio-rebalancer/internal/models"
)

// Store is the persistence surface used by HTTP and worker handlers.
type Store interface {
	SavePortfolio(ctx context.Context, p models.Portfolio) error
	GetPortfolio(ctx context.Context, userID string) (*models.Portfolio, error)
	ListPortfolios(ctx context.Context) ([]models.Portfolio, error)
	SaveTransaction(ctx context.Context, t models.Transaction) error
	ListTransactions(ctx context.Context, userID string) ([]models.Transaction, error)
	SaveRebalance(ctx context.Context, p models.Portfolio, txs []models.Transaction) error
}
