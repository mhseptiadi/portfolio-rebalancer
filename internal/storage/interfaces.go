package storage

import (
	"context"
	"portfolio-rebalancer/internal/models"
)

type PortfolioRepository interface {
	GetPortfolio(ctx context.Context, id string) (models.Portfolio, error)
	ListPortfolios(ctx context.Context) ([]models.Portfolio, error)
	SavePortfolio(ctx context.Context, p models.Portfolio) error
}

type TransactionRepository interface {
	GetTransaction(ctx context.Context, id string) (models.Transaction, error)
	SaveTransaction(ctx context.Context, t models.Transaction) error
	ListTransactions(ctx context.Context, portfolioID string) ([]models.Transaction, error)
	ListAllTransactions(ctx context.Context) ([]models.Transaction, error)
}
