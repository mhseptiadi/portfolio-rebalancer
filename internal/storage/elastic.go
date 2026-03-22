package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"portfolio-rebalancer/internal/models"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// var esClient *elasticsearch.Client

type ElasticStorage struct {
	esClient          *elasticsearch.Client
	portfoliosIndex   string
	transactionsIndex string
}

var _ Store = (*ElasticStorage)(nil)

// InitElastic initializes elasticsearch connection with retry logic
func InitElastic(elasticsearchURL string) (*ElasticStorage, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			elasticsearchURL,
		},
	}

	var client *elasticsearch.Client
	var err error
	es := &ElasticStorage{
		esClient:          client,
		portfoliosIndex:   "portfolios",
		transactionsIndex: "transactions",
	}

	for i := 1; i <= 5; i++ {
		client, err = elasticsearch.NewClient(cfg)
		if err != nil {
			log.Printf("Failed to create client: %v", err)
		} else {
			_, err = client.Info()
			if err == nil {
				log.Println("Connected to Elasticsearch")
				es.esClient = client
				return es, nil
			}
			log.Printf("Client created, but ES not ready: %v", err)
		}

		log.Printf("Retrying connection to Elasticsearch... (%d/5)", i)
		time.Sleep(5 * time.Second)
	}

	return nil, fmt.Errorf("failed to connect to Elasticsearch after retries: %w", err)
}

// createIndicesWithMapping defines the exact schema for our models
func (es *ElasticStorage) createIndicesWithMapping(ctx context.Context) error {
	// 1. Transaction Mapping (Notice created_at is strictly a "date")
	txMapping := `{
		"mappings": {
			"properties": {
				"id": { "type": "keyword" },
				"user_id": { "type": "keyword" },
				"type": { "type": "keyword" },
				"allocation_type": { "type": "keyword" },
				"amount": { "type": "integer" },
				"order": { "type": "integer" },
				"created_at": { "type": "date" },
				"updated_at": { "type": "date" }
			}
		}
	}`

	// 2. Portfolio Mapping (Handling the nested struct)
	portfolioMapping := `{
		"mappings": {
			"properties": {
				"user_id": { "type": "keyword" },
				"allocation": {
					"properties": {
						"stocks": { "type": "integer" },
						"bonds": { "type": "integer" },
						"gold": { "type": "integer" }
					}
				},
				"created_at": { "type": "date" },
				"updated_at": { "type": "date" }
			}
		}
	}`

	if err := es.createIndexIfNotExists(ctx, es.transactionsIndex, txMapping); err != nil {
		return err
	}
	if err := es.createIndexIfNotExists(ctx, es.portfoliosIndex, portfolioMapping); err != nil {
		return err
	}

	return nil
}

func (es *ElasticStorage) createIndexIfNotExists(ctx context.Context, indexName, mapping string) error {
	// Check if the index already exists
	existsRes, err := esapi.IndicesExistsRequest{
		Index: []string{indexName},
	}.Do(ctx, es.esClient)
	if err != nil {
		return err
	}
	defer existsRes.Body.Close()

	if existsRes.StatusCode == 200 {
		return nil
	}

	createRes, err := esapi.IndicesCreateRequest{
		Index: indexName,
		Body:  bytes.NewReader([]byte(mapping)),
	}.Do(ctx, es.esClient)
	if err != nil {
		return err
	}
	defer createRes.Body.Close()

	if createRes.IsError() {
		return fmt.Errorf("error creating index %s: %s", indexName, createRes.Status())
	}

	return nil
}

func (es *ElasticStorage) SavePortfolio(ctx context.Context, p models.Portfolio) error {
	body, err := json.Marshal(p)
	if err != nil {
		return err
	}

	res, err := es.esClient.Index("portfolios", bytes.NewReader(body), es.esClient.Index.WithDocumentID(p.UserID))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error saving portfolio: %s", res.String())
	}

	log.Printf("Portfolio saved for user %s", p.UserID)
	return nil
}

func (es *ElasticStorage) GetPortfolio(ctx context.Context, userID string) (*models.Portfolio, error) {
	res, err := es.esClient.Get("portfolios", userID)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("user not found")
	}

	var esResp struct {
		Source models.Portfolio `json:"_source"`
	}

	if err := json.NewDecoder(res.Body).Decode(&esResp); err != nil {
		return nil, err
	}

	return &esResp.Source, nil
}

func (es *ElasticStorage) ListPortfolios(ctx context.Context) ([]models.Portfolio, error) {
	body := `{"query":{"match_all":{}},"size":10000}`
	res, err := es.esClient.Search(
		es.esClient.Search.WithContext(ctx),
		es.esClient.Search.WithIndex(es.portfoliosIndex),
		es.esClient.Search.WithBody(bytes.NewReader([]byte(body))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error listing portfolios: %s", res.String())
	}

	var searchResp struct {
		Hits struct {
			Hits []struct {
				Source models.Portfolio `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&searchResp); err != nil {
		return nil, err
	}

	out := make([]models.Portfolio, 0, len(searchResp.Hits.Hits))
	for _, h := range searchResp.Hits.Hits {
		out = append(out, h.Source)
	}
	return out, nil
}

func normalizeTransactionTimestamps(t models.Transaction) models.Transaction {
	if t.UpdatedAt.IsZero() {
		t.UpdatedAt = t.CreatedAt
		if t.UpdatedAt.IsZero() {
			t.UpdatedAt = time.Now().UTC()
		}
	}
	return t
}

func (es *ElasticStorage) SaveTransaction(ctx context.Context, t models.Transaction) error {
	t = normalizeTransactionTimestamps(t)

	body, err := json.Marshal(t)
	if err != nil {
		return err
	}

	res, err := es.esClient.Index(
		es.transactionsIndex,
		bytes.NewReader(body),
		es.esClient.Index.WithContext(ctx),
		es.esClient.Index.WithDocumentID(t.ID),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error saving transaction: %s", res.String())
	}

	log.Printf("Transaction saved id=%s user=%s", t.ID, t.UserID)
	return nil
}

func (es *ElasticStorage) SaveRebalance(ctx context.Context, p models.Portfolio, txs []models.Transaction) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)

	for _, t := range txs {
		t = normalizeTransactionTimestamps(t)
		meta := map[string]map[string]string{
			"index": {
				"_index": es.transactionsIndex,
				"_id":    t.ID,
			},
		}
		if err := enc.Encode(meta); err != nil {
			return err
		}
		if err := enc.Encode(t); err != nil {
			return err
		}
	}

	pmeta := map[string]map[string]string{
		"index": {
			"_index": es.portfoliosIndex,
			"_id":    p.UserID,
		},
	}
	if err := enc.Encode(pmeta); err != nil {
		return err
	}
	if err := enc.Encode(p); err != nil {
		return err
	}

	res, err := esapi.BulkRequest{
		Body:    &buf,
		Refresh: "wait_for",
	}.Do(ctx, es.esClient)
	if err != nil {
		return fmt.Errorf("failed to save rebalance: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to save rebalance: %s", res.String())
	}

	var bulkResp struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Status int `json:"status"`
			Error  struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := json.NewDecoder(res.Body).Decode(&bulkResp); err != nil {
		return fmt.Errorf("failed to decode bulk response: %w", err)
	}

	if bulkResp.Errors {
		var reasons []string
		for _, item := range bulkResp.Items {
			for _, op := range item {
				if op.Status >= 300 {
					if op.Error.Reason != "" {
						reasons = append(reasons, op.Error.Reason)
					} else {
						reasons = append(reasons, fmt.Sprintf("status %d", op.Status))
					}
				}
			}
		}
		return fmt.Errorf("failed to save rebalance: %s", strings.Join(reasons, "; "))
	}

	log.Printf("Rebalance saved user=%s txs=%d", p.UserID, len(txs))
	return nil
}

func (es *ElasticStorage) ListTransactions(ctx context.Context, portfolioID string) ([]models.Transaction, error) {
	queryBody := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"user_id": portfolioID,
			},
		},
		"sort": []interface{}{
			map[string]string{"created_at": "desc"},
			map[string]string{"order": "desc"},
		},
		"size": 10000,
	}
	q, err := json.Marshal(queryBody)
	if err != nil {
		return nil, err
	}

	res, err := es.esClient.Search(
		es.esClient.Search.WithContext(ctx),
		es.esClient.Search.WithIndex(es.transactionsIndex),
		es.esClient.Search.WithBody(bytes.NewReader(q)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error listing transactions: %s", res.String())
	}

	var searchResp struct {
		Hits struct {
			Hits []struct {
				Source models.Transaction `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&searchResp); err != nil {
		return nil, err
	}

	out := make([]models.Transaction, 0, len(searchResp.Hits.Hits))
	for _, h := range searchResp.Hits.Hits {
		out = append(out, h.Source)
	}
	return out, nil
}
