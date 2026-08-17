package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	ErrUnknownTaskType = errors.New("unknown task adapter type")
	ErrAdapterFailed   = errors.New("task adapter execution failed")
)

// TaskAdapter defines the contract for executing an individual task node.
type TaskAdapter interface {
	Execute(ctx context.Context, config map[string]any) (string, error)
}

// AdapterRegistry manages the available task execution adapters.
type AdapterRegistry struct {
	mu       sync.RWMutex
	adapters map[string]TaskAdapter
}

// NewDefaultRegistry creates a registry pre-loaded with synthetic and HTTP adapters.
func NewDefaultRegistry() *AdapterRegistry {
	r := &AdapterRegistry{
		adapters: make(map[string]TaskAdapter),
	}
	r.Register("synthetic", &SyntheticAdapter{})
	r.Register("http", &HTTPAdapter{client: &http.Client{Timeout: 30 * time.Second}})
	return r
}

// Register adds a new adapter to the registry.
func (r *AdapterRegistry) Register(taskType string, adapter TaskAdapter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adapters[taskType] = adapter
}

// Get retrieves an adapter by task type name.
func (r *AdapterRegistry) Get(taskType string) (TaskAdapter, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	adapter, exists := r.adapters[taskType]
	return adapter, exists
}

// SyntheticAdapter simulates compute workloads, delays, and deliberate chaos errors.
type SyntheticAdapter struct{}

func (s *SyntheticAdapter) Execute(ctx context.Context, config map[string]any) (string, error) {
	// 1. Simulate work latency / sleep
	if sleepMsVal, ok := config["sleep_ms"]; ok {
		var sleepMs int
		switch v := sleepMsVal.(type) {
		case int:
			sleepMs = v
		case float64:
			sleepMs = int(v)
		}
		if sleepMs > 0 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(time.Duration(sleepMs) * time.Millisecond):
			}
		}
	}

	// 2. Simulate forced failure injection
	if shouldFail, ok := config["should_fail"].(bool); ok && shouldFail {
		errMsg := "synthetic forced error"
		if msg, ok := config["error_message"].(string); ok && msg != "" {
			errMsg = msg
		}
		return "", fmt.Errorf("%w: %s", ErrAdapterFailed, errMsg)
	}

	// 3. Return output payload
	if result, ok := config["result"].(string); ok {
		return result, nil
	}

	return `{"status":"synthetic_success"}`, nil
}

// HTTPAdapter executes outbound HTTP requests.
type HTTPAdapter struct {
	client *http.Client
}

func (h *HTTPAdapter) Execute(ctx context.Context, config map[string]any) (string, error) {
	urlStr, ok := config["url"].(string)
	if !ok || urlStr == "" {
		return "", errors.New("http adapter requires 'url' config")
	}

	method := "GET"
	if m, ok := config["method"].(string); ok && m != "" {
		method = strings.ToUpper(m)
	}

	var reqBody io.Reader
	if bodyStr, ok := config["body"].(string); ok && bodyStr != "" {
		reqBody = bytes.NewBufferString(bodyStr)
	}

	req, err := http.NewRequestWithContext(ctx, method, urlStr, reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}

	if headers, ok := config["headers"].(map[string]any); ok {
		for k, v := range headers {
			if strVal, ok := v.(string); ok {
				req.Header.Set(k, strVal)
			}
		}
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("%w: HTTP status %d: %s", ErrAdapterFailed, resp.StatusCode, string(respBytes))
	}

	return string(respBytes), nil
}
