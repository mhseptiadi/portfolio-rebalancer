package factory

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"portfolio-rebalancer/internal/handlers"
	"portfolio-rebalancer/internal/kafka"
	"portfolio-rebalancer/internal/models"
	"portfolio-rebalancer/internal/storage"
)

// MemoryStore is an in-memory storage.Store for HTTP/worker e2e tests.
type MemoryStore struct {
	mu sync.Mutex

	Portfolios map[string]models.Portfolio
	Txs        []models.Transaction

	ErrSavePortfolio    error
	ErrGetPortfolio     error
	ErrListPortfolios   error
	ErrSaveTransaction  error
	ErrListTransactions error
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{Portfolios: make(map[string]models.Portfolio)}
}

func (m *MemoryStore) SavePortfolio(ctx context.Context, p models.Portfolio) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ErrSavePortfolio != nil {
		return m.ErrSavePortfolio
	}
	m.Portfolios[p.UserID] = p
	return nil
}

func (m *MemoryStore) GetPortfolio(ctx context.Context, userID string) (*models.Portfolio, error) {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ErrGetPortfolio != nil {
		return nil, m.ErrGetPortfolio
	}
	p, ok := m.Portfolios[userID]
	if !ok {
		return nil, fmt.Errorf("user not found")
	}
	copy := p
	return &copy, nil
}

func (m *MemoryStore) ListPortfolios(ctx context.Context) ([]models.Portfolio, error) {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ErrListPortfolios != nil {
		return nil, m.ErrListPortfolios
	}
	out := make([]models.Portfolio, 0, len(m.Portfolios))
	for _, p := range m.Portfolios {
		out = append(out, p)
	}
	return out, nil
}

func (m *MemoryStore) SaveTransaction(ctx context.Context, t models.Transaction) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ErrSaveTransaction != nil {
		return m.ErrSaveTransaction
	}
	m.Txs = append(m.Txs, t)
	return nil
}

func (m *MemoryStore) SaveRebalance(ctx context.Context, p models.Portfolio, txs []models.Transaction) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ErrSaveTransaction != nil {
		return fmt.Errorf("failed to save transaction")
	}
	if m.ErrSavePortfolio != nil {
		return fmt.Errorf("failed to save new portfolio")
	}
	for _, t := range txs {
		m.Txs = append(m.Txs, t)
	}
	m.Portfolios[p.UserID] = p
	return nil
}

func (m *MemoryStore) ListTransactions(ctx context.Context, userID string) ([]models.Transaction, error) {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ErrListTransactions != nil {
		return nil, m.ErrListTransactions
	}
	var out []models.Transaction
	for _, t := range m.Txs {
		if t.UserID == userID {
			out = append(out, t)
		}
	}
	return out, nil
}

// MockPublisher records PublishMessage calls and optional errors.
type MockPublisher struct {
	mu         sync.Mutex
	PublishErr error
	Payloads   [][]byte
}

func (m *MockPublisher) PublishMessage(ctx context.Context, payload []byte) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.PublishErr != nil {
		return m.PublishErr
	}
	m.Payloads = append(m.Payloads, append([]byte(nil), payload...))
	return nil
}

func (m *MockPublisher) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.PublishErr = nil
	m.Payloads = nil
}

func (m *MockPublisher) LastPayload() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.Payloads) == 0 {
		return nil
	}
	return m.Payloads[len(m.Payloads)-1]
}

// TestHarness matches cmd/api routing with injectable store and publisher.
type TestHarness struct {
	Store   *MemoryStore
	Publish *MockPublisher
	Handler *handlers.PortfolioHandler
	Mux     http.Handler
}

// NewTestHarness builds handler + mux wired like cmd/api (port 8083 in production).
func NewTestHarness() *TestHarness {
	st := NewMemoryStore()
	pub := &MockPublisher{}
	h := handlers.NewPortfolioHandler(st, pub)
	mux := http.NewServeMux()
	mux.HandleFunc("/portfolio", h.SavePortfolio)
	mux.HandleFunc("/portfolio/id", h.GetPortfolio)
	mux.HandleFunc("/portfolios", h.ListPortfolios)
	mux.HandleFunc("/rebalance", h.HandleRebalance)
	mux.HandleFunc("/transactions", h.ListTransactions)
	return &TestHarness{Store: st, Publish: pub, Handler: h, Mux: mux}
}

// NewTestHarnessNoKafka rebalance HTTP returns 500 (kafka is not configured).
func NewTestHarnessNoKafka() *TestHarness {
	st := NewMemoryStore()
	h := handlers.NewPortfolioHandler(st, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("/portfolio", h.SavePortfolio)
	mux.HandleFunc("/portfolio/id", h.GetPortfolio)
	mux.HandleFunc("/portfolios", h.ListPortfolios)
	mux.HandleFunc("/rebalance", h.HandleRebalance)
	mux.HandleFunc("/transactions", h.ListTransactions)
	return &TestHarness{Store: st, Publish: nil, Handler: h, Mux: mux}
}

// Ensure interfaces stay aligned if signatures change.
var (
	_ storage.Store          = (*MemoryStore)(nil)
	_ kafka.MessagePublisher = (*MockPublisher)(nil)
)
