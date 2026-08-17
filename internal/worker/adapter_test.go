package worker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSyntheticAdapter_Success(t *testing.T) {
	adapter := &SyntheticAdapter{}
	ctx := context.Background()

	config := map[string]any{
		"sleep_ms": 10,
		"result":   `{"data": 42}`,
	}

	res, err := adapter.Execute(ctx, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != `{"data": 42}` {
		t.Errorf("expected result `{\"data\": 42}`, got %s", res)
	}
}

func TestSyntheticAdapter_Failure(t *testing.T) {
	adapter := &SyntheticAdapter{}
	ctx := context.Background()

	config := map[string]any{
		"should_fail":   true,
		"error_message": "database connection timeout",
	}

	_, err := adapter.Execute(ctx, config)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, ErrAdapterFailed) {
		t.Errorf("expected ErrAdapterFailed, got %v", err)
	}
}

func TestSyntheticAdapter_ContextCancellation(t *testing.T) {
	adapter := &SyntheticAdapter{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	config := map[string]any{
		"sleep_ms": 500, // sleep longer than context timeout
	}

	_, err := adapter.Execute(ctx, config)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestHTTPAdapter_Execution(t *testing.T) {
	// Mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Custom-Header") != "FlowForge" {
			http.Error(w, "missing header", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"received": true}`))
	}))
	defer server.Close()

	adapter := &HTTPAdapter{client: server.Client()}
	ctx := context.Background()

	config := map[string]any{
		"url":    server.URL,
		"method": "POST",
		"headers": map[string]any{
			"X-Custom-Header": "FlowForge",
		},
		"body": `{"msg":"hello"}`,
	}

	res, err := adapter.Execute(ctx, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != `{"received": true}` {
		t.Errorf("expected response `{\"received\": true}`, got %s", res)
	}
}
