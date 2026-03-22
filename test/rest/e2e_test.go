package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"portfolio-rebalancer/internal/models"
	"portfolio-rebalancer/test/factory"
)

const applicationJSON = "application/json"

func jsonBody(t *testing.T, v any) io.Reader {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.NewReader(b)
}

func TestCreatePortfolio(t *testing.T) {
	t.Run("success when user_id has no portfolio", func(t *testing.T) {
		h := factory.NewTestHarness()
		body := map[string]any{
			"user_id": "user-e2e-new",
			"allocation": map[string]int{
				"stocks": 60, "bonds": 30, "gold": 10,
			},
		}
		req := httptest.NewRequest(http.MethodPost, "/portfolio", jsonBody(t, body))
		req.Header.Set("Content-Type", applicationJSON)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusCreated {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
		var got models.Portfolio
		if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		if got.UserID != "user-e2e-new" {
			t.Errorf("user_id = %q", got.UserID)
		}
		if got.Allocation.Stocks != 60 || got.Allocation.Bonds != 30 || got.Allocation.Gold != 10 {
			t.Errorf("allocation = %+v", got.Allocation)
		}
	})

	t.Run("failed when user_id already exists", func(t *testing.T) {
		h := factory.NewTestHarness()
		body := map[string]any{
			"user_id": "user-dup",
			"allocation": map[string]int{
				"stocks": 60, "bonds": 30, "gold": 10,
			},
		}
		req1 := httptest.NewRequest(http.MethodPost, "/portfolio", jsonBody(t, body))
		req1.Header.Set("Content-Type", applicationJSON)
		h.Mux.ServeHTTP(httptest.NewRecorder(), req1)

		req2 := httptest.NewRequest(http.MethodPost, "/portfolio", jsonBody(t, body))
		req2.Header.Set("Content-Type", applicationJSON)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req2)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
		b, _ := io.ReadAll(res.Body)
		if !strings.Contains(string(b), "User already exists") {
			t.Fatalf("body = %q", string(b))
		}
	})

	t.Run("failed when method is not POST", func(t *testing.T) {
		h := factory.NewTestHarness()
		req := httptest.NewRequest(http.MethodGet, "/portfolio", nil)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("status %d", res.StatusCode)
		}
	})

	t.Run("failed when allocation is invalid", func(t *testing.T) {
		h := factory.NewTestHarness()
		body := map[string]any{
			"user_id": "user-bad-alloc",
			"allocation": map[string]int{
				"stocks": 50, "bonds": 30, "gold": 10,
			},
		}
		req := httptest.NewRequest(http.MethodPost, "/portfolio", jsonBody(t, body))
		req.Header.Set("Content-Type", applicationJSON)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
		b, _ := io.ReadAll(res.Body)
		if !strings.Contains(string(b), "Invalid allocation") {
			t.Fatalf("body = %q", string(b))
		}
	})

	t.Run("failed to save portfolio returns 500", func(t *testing.T) {
		h := factory.NewTestHarness()
		h.Store.ErrSavePortfolio = io.EOF
		body := map[string]any{
			"user_id": "user-save-fail",
			"allocation": map[string]int{
				"stocks": 60, "bonds": 30, "gold": 10,
			},
		}
		req := httptest.NewRequest(http.MethodPost, "/portfolio", jsonBody(t, body))
		req.Header.Set("Content-Type", applicationJSON)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusInternalServerError {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
	})
}

func TestGetPortfolioByID(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		h := factory.NewTestHarness()
		_ = h.Store.SavePortfolio(context.Background(), models.Portfolio{
			UserID: "user-get-ok",
			Allocation: models.Allocation{
				Stocks: 60, Bonds: 30, Gold: 10,
			},
		})

		req := httptest.NewRequest(http.MethodGet, "/portfolio/id?user_id=user-get-ok", nil)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
		var got models.Portfolio
		if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		if got.UserID != "user-get-ok" {
			t.Errorf("user_id = %q", got.UserID)
		}
	})

	t.Run("failed not found", func(t *testing.T) {
		h := factory.NewTestHarness()
		req := httptest.NewRequest(http.MethodGet, "/portfolio/id?user_id=nobody", nil)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
		b, _ := io.ReadAll(res.Body)
		if !strings.Contains(string(b), "Portfolio not found") {
			t.Fatalf("body = %q", string(b))
		}
	})

	t.Run("failed when method is not GET", func(t *testing.T) {
		h := factory.NewTestHarness()
		req := httptest.NewRequest(http.MethodPost, "/portfolio/id?user_id=x", nil)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("status %d", res.StatusCode)
		}
	})

	t.Run("failed when user_id query is missing", func(t *testing.T) {
		h := factory.NewTestHarness()
		req := httptest.NewRequest(http.MethodGet, "/portfolio/id", nil)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			t.Fatalf("status %d", res.StatusCode)
		}
	})
}

func TestListPortfolios(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		h := factory.NewTestHarness()
		_ = h.Store.SavePortfolio(context.Background(), models.Portfolio{
			UserID: "user-list-1",
			Allocation: models.Allocation{
				Stocks: 60, Bonds: 30, Gold: 10,
			},
		})

		req := httptest.NewRequest(http.MethodGet, "/portfolios", nil)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
		var list []models.Portfolio
		if err := json.NewDecoder(res.Body).Decode(&list); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, p := range list {
			if p.UserID == "user-list-1" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing portfolio: %+v", list)
		}
	})

	t.Run("failed when method is not GET", func(t *testing.T) {
		h := factory.NewTestHarness()
		req := httptest.NewRequest(http.MethodPost, "/portfolios", nil)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("status %d", res.StatusCode)
		}
	})
}

func TestRebalanceHTTP(t *testing.T) {
	t.Run("success publishes message", func(t *testing.T) {
		h := factory.NewTestHarness()
		_ = h.Store.SavePortfolio(context.Background(), models.Portfolio{
			UserID: "user-reb-http",
			Allocation: models.Allocation{
				Stocks: 60, Bonds: 30, Gold: 10,
			},
		})
		body := map[string]any{
			"user_id": "user-reb-http",
			"new_allocation": map[string]int{
				"stocks": 70, "bonds": 20, "gold": 10,
			},
		}
		req := httptest.NewRequest(http.MethodPost, "/rebalance", jsonBody(t, body))
		req.Header.Set("Content-Type", applicationJSON)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
		if len(h.Publish.Payloads) != 1 {
			t.Fatalf("publish calls = %d", len(h.Publish.Payloads))
		}
	})

	t.Run("failed when method is not POST", func(t *testing.T) {
		h := factory.NewTestHarness()
		req := httptest.NewRequest(http.MethodGet, "/rebalance", nil)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("status %d", res.StatusCode)
		}
	})

	t.Run("failed when user_id is required", func(t *testing.T) {
		h := factory.NewTestHarness()
		body := map[string]any{
			"user_id": "",
			"new_allocation": map[string]int{
				"stocks": 70, "bonds": 20, "gold": 10,
			},
		}
		req := httptest.NewRequest(http.MethodPost, "/rebalance", jsonBody(t, body))
		req.Header.Set("Content-Type", applicationJSON)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
	})

	t.Run("failed when allocation is invalid", func(t *testing.T) {
		h := factory.NewTestHarness()
		_ = h.Store.SavePortfolio(context.Background(), models.Portfolio{
			UserID: "user-reb-bad",
			Allocation: models.Allocation{
				Stocks: 60, Bonds: 30, Gold: 10,
			},
		})
		body := map[string]any{
			"user_id": "user-reb-bad",
			"new_allocation": map[string]int{
				"stocks": 40, "bonds": 40, "gold": 10,
			},
		}
		req := httptest.NewRequest(http.MethodPost, "/rebalance", jsonBody(t, body))
		req.Header.Set("Content-Type", applicationJSON)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
	})

	t.Run("failed when portfolio not found", func(t *testing.T) {
		h := factory.NewTestHarness()
		body := map[string]any{
			"user_id": "missing-user",
			"new_allocation": map[string]int{
				"stocks": 70, "bonds": 20, "gold": 10,
			},
		}
		req := httptest.NewRequest(http.MethodPost, "/rebalance", jsonBody(t, body))
		req.Header.Set("Content-Type", applicationJSON)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
	})

	t.Run("failed when get portfolio returns error", func(t *testing.T) {
		h := factory.NewTestHarness()
		h.Store.ErrGetPortfolio = io.ErrUnexpectedEOF
		body := map[string]any{
			"user_id": "any",
			"new_allocation": map[string]int{
				"stocks": 70, "bonds": 20, "gold": 10,
			},
		}
		req := httptest.NewRequest(http.MethodPost, "/rebalance", jsonBody(t, body))
		req.Header.Set("Content-Type", applicationJSON)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusInternalServerError {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
	})

	t.Run("failed when kafka is not configured", func(t *testing.T) {
		h := factory.NewTestHarnessNoKafka()
		_ = h.Store.SavePortfolio(context.Background(), models.Portfolio{
			UserID: "user-no-kafka",
			Allocation: models.Allocation{
				Stocks: 60, Bonds: 30, Gold: 10,
			},
		})
		body := map[string]any{
			"user_id": "user-no-kafka",
			"new_allocation": map[string]int{
				"stocks": 70, "bonds": 20, "gold": 10,
			},
		}
		req := httptest.NewRequest(http.MethodPost, "/rebalance", jsonBody(t, body))
		req.Header.Set("Content-Type", applicationJSON)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusInternalServerError {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
		b, _ := io.ReadAll(res.Body)
		if !strings.Contains(string(b), "kafka is not configured") {
			t.Fatalf("body = %q", string(b))
		}
	})

	t.Run("failed to enqueue when publish fails", func(t *testing.T) {
		h := factory.NewTestHarness()
		h.Publish.PublishErr = io.ErrClosedPipe
		_ = h.Store.SavePortfolio(context.Background(), models.Portfolio{
			UserID: "user-pub-fail",
			Allocation: models.Allocation{
				Stocks: 60, Bonds: 30, Gold: 10,
			},
		})
		body := map[string]any{
			"user_id": "user-pub-fail",
			"new_allocation": map[string]int{
				"stocks": 70, "bonds": 20, "gold": 10,
			},
		}
		req := httptest.NewRequest(http.MethodPost, "/rebalance", jsonBody(t, body))
		req.Header.Set("Content-Type", applicationJSON)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusInternalServerError {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
		b, _ := io.ReadAll(res.Body)
		if !strings.Contains(string(b), "failed to enqueue rebalance") {
			t.Fatalf("body = %q", string(b))
		}
	})

	t.Run("failed when request body is invalid JSON", func(t *testing.T) {
		h := factory.NewTestHarness()
		req := httptest.NewRequest(http.MethodPost, "/rebalance", strings.NewReader("not-json"))
		req.Header.Set("Content-Type", applicationJSON)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
	})
}

func TestListTransactions(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		h := factory.NewTestHarness()
		_ = h.Store.SavePortfolio(context.Background(), models.Portfolio{
			UserID: "user-tx",
			Allocation: models.Allocation{
				Stocks: 60, Bonds: 30, Gold: 10,
			},
		})
		rebBody := map[string]any{
			"user_id": "user-tx",
			"new_allocation": map[string]int{
				"stocks": 70, "bonds": 20, "gold": 10,
			},
		}
		req := httptest.NewRequest(http.MethodPost, "/rebalance", jsonBody(t, rebBody))
		req.Header.Set("Content-Type", applicationJSON)
		h.Mux.ServeHTTP(httptest.NewRecorder(), req)
		if err := h.Handler.HandleRebalanceMessage(h.Publish.LastPayload()); err != nil {
			t.Fatal(err)
		}

		reqList := httptest.NewRequest(http.MethodGet, "/transactions?user_id=user-tx", nil)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, reqList)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(res.Body)
			t.Fatalf("status %d, body %s", res.StatusCode, b)
		}
		var txs []models.Transaction
		if err := json.NewDecoder(res.Body).Decode(&txs); err != nil {
			t.Fatal(err)
		}
		if len(txs) == 0 {
			t.Fatal("expected transactions")
		}
	})

	t.Run("failed when method is not GET", func(t *testing.T) {
		h := factory.NewTestHarness()
		req := httptest.NewRequest(http.MethodPost, "/transactions?user_id=u", nil)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("status %d", res.StatusCode)
		}
	})

	t.Run("failed when user_id is required", func(t *testing.T) {
		h := factory.NewTestHarness()
		req := httptest.NewRequest(http.MethodGet, "/transactions", nil)
		rec := httptest.NewRecorder()
		h.Mux.ServeHTTP(rec, req)
		res := rec.Result()
		defer res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			t.Fatalf("status %d", res.StatusCode)
		}
		b, _ := io.ReadAll(res.Body)
		if !strings.Contains(string(b), "user_id is required") {
			t.Fatalf("body = %q", string(b))
		}
	})
}

func TestWorkerHandleRebalanceMessage(t *testing.T) {
	t.Run("success updates portfolio and writes transactions", func(t *testing.T) {
		h := factory.NewTestHarness()
		_ = h.Store.SavePortfolio(context.Background(), models.Portfolio{
			UserID: "worker-ok",
			Allocation: models.Allocation{
				Stocks: 60, Bonds: 30, Gold: 10,
			},
		})
		msg, _ := json.Marshal(models.UpdatedPortfolio{
			UserID: "worker-ok",
			NewAllocation: models.Allocation{
				Stocks: 70, Bonds: 15, Gold: 15,
			},
		})
		if err := h.Handler.HandleRebalanceMessage(msg); err != nil {
			t.Fatal(err)
		}
		p, err := h.Store.GetPortfolio(context.Background(), "worker-ok")
		if err != nil {
			t.Fatal(err)
		}
		if p.Allocation.Stocks != 70 {
			t.Fatalf("stocks = %d", p.Allocation.Stocks)
		}
		txs, err := h.Store.ListTransactions(context.Background(), "worker-ok")
		if err != nil {
			t.Fatal(err)
		}
		if len(txs) == 0 {
			t.Fatal("expected transactions")
		}

		// check if transactions are selling bonds 15 and buying stocks 10 and gold 5
		if txs[0].Type != "SELL" || txs[0].AllocationType != "bonds" || txs[0].Amount != 15 {
			t.Fatalf("expected sell bonds 15, got %+v", txs[0])
		}
		if txs[1].Type != "BUY" || txs[1].AllocationType != "stocks" || txs[1].Amount != 10 {
			t.Fatalf("expected buy stocks 10, got %+v", txs[1])
		}
		if txs[2].Type != "BUY" || txs[2].AllocationType != "gold" || txs[2].Amount != 5 {
			t.Fatalf("expected buy gold 15, got %+v", txs[2])
		}
	})

	t.Run("failed to unmarshal returns error", func(t *testing.T) {
		h := factory.NewTestHarness()
		err := h.Handler.HandleRebalanceMessage([]byte(`{`))
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("invalid allocation returns nil (logged, skipped)", func(t *testing.T) {
		h := factory.NewTestHarness()
		_ = h.Store.SavePortfolio(context.Background(), models.Portfolio{
			UserID: "worker-bad-alloc",
			Allocation: models.Allocation{
				Stocks: 60, Bonds: 30, Gold: 10,
			},
		})
		msg, _ := json.Marshal(map[string]any{
			"user_id": "worker-bad-alloc",
			"new_allocation": map[string]int{
				"stocks": 10, "bonds": 10, "gold": 10,
			},
		})
		if err := h.Handler.HandleRebalanceMessage(msg); err != nil {
			t.Fatalf("got %v; handler returns nil for invalid allocation", err)
		}
	})

	t.Run("empty user_id returns nil", func(t *testing.T) {
		h := factory.NewTestHarness()
		msg, _ := json.Marshal(map[string]any{
			"user_id": "",
			"new_allocation": map[string]int{
				"stocks": 60, "bonds": 30, "gold": 10,
			},
		})
		if err := h.Handler.HandleRebalanceMessage(msg); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("portfolio not found returns nil", func(t *testing.T) {
		h := factory.NewTestHarness()
		msg, _ := json.Marshal(models.UpdatedPortfolio{
			UserID: "nope",
			NewAllocation: models.Allocation{
				Stocks: 70, Bonds: 20, Gold: 10,
			},
		})
		if err := h.Handler.HandleRebalanceMessage(msg); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("failed to save transaction", func(t *testing.T) {
		h := factory.NewTestHarness()
		h.Store.ErrSaveTransaction = io.ErrShortBuffer
		_ = h.Store.SavePortfolio(context.Background(), models.Portfolio{
			UserID: "worker-tx-fail",
			Allocation: models.Allocation{
				Stocks: 60, Bonds: 30, Gold: 10,
			},
		})
		msg, _ := json.Marshal(models.UpdatedPortfolio{
			UserID: "worker-tx-fail",
			NewAllocation: models.Allocation{
				Stocks: 70, Bonds: 20, Gold: 10,
			},
		})
		err := h.Handler.HandleRebalanceMessage(msg)
		if err == nil || !strings.Contains(err.Error(), "failed to save transaction") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("failed to save new portfolio", func(t *testing.T) {
		h := factory.NewTestHarness()
		_ = h.Store.SavePortfolio(context.Background(), models.Portfolio{
			UserID: "worker-save-p-fail",
			Allocation: models.Allocation{
				Stocks: 60, Bonds: 30, Gold: 10,
			},
		})
		h.Store.ErrSavePortfolio = io.ErrNoProgress
		msg, _ := json.Marshal(models.UpdatedPortfolio{
			UserID: "worker-save-p-fail",
			NewAllocation: models.Allocation{
				Stocks: 70, Bonds: 20, Gold: 10,
			},
		})
		err := h.Handler.HandleRebalanceMessage(msg)
		if err == nil || !strings.Contains(err.Error(), "failed to save new portfolio") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("same allocation is no-op success", func(t *testing.T) {
		h := factory.NewTestHarness()
		_ = h.Store.SavePortfolio(context.Background(), models.Portfolio{
			UserID: "worker-same",
			Allocation: models.Allocation{
				Stocks: 60, Bonds: 30, Gold: 10,
			},
		})
		msg, _ := json.Marshal(models.UpdatedPortfolio{
			UserID: "worker-same",
			NewAllocation: models.Allocation{
				Stocks: 60, Bonds: 30, Gold: 10,
			},
		})
		if err := h.Handler.HandleRebalanceMessage(msg); err != nil {
			t.Fatal(err)
		}
		txs, _ := h.Store.ListTransactions(context.Background(), "worker-same")
		if len(txs) != 0 {
			t.Fatalf("expected no txs, got %d", len(txs))
		}
	})
}
