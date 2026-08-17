package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/flowforge/flowforge/internal/repository"
	"github.com/flowforge/flowforge/pkg/domain"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	testDBConnString = "postgres://flowforge:flowforge_password@localhost:5433/flowforge?sslmode=disable"
	testRedisAddr    = "localhost:6379"
)

func setupTestServer(t *testing.T) (*httptest.Server, *pgxpool.Pool, *redis.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, testDBConnString)
	if err != nil {
		t.Skipf("skipping test: postgres unreachable: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping test: postgres ping failed: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("skipping test: redis unreachable: %v", err)
	}

	repo := repository.NewPostgresRepository(pool)
	wsHub := NewWebSocketHub()
	server := NewServer(pool, repo, rdb, wsHub)

	ts := httptest.NewServer(server.Router())
	return ts, pool, rdb
}

func TestAPI_Health(t *testing.T) {
	ts, pool, rdb := setupTestServer(t)
	defer ts.Close()
	defer pool.Close()
	defer rdb.Close()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

func TestAPI_WorkflowSubmission_And_CycleRejection(t *testing.T) {
	ts, pool, rdb := setupTestServer(t)
	defer ts.Close()
	defer pool.Close()
	defer rdb.Close()

	// 1. Submit Cyclic Workflow -> should be rejected with 400 Bad Request
	cyclicDef := domain.WorkflowDefinition{
		ID:   "cyclic-api-test",
		Name: "Cyclic Test",
		Nodes: map[string]domain.Node{
			"nodeA": {ID: "nodeA", Type: "synthetic"},
			"nodeB": {ID: "nodeB", Type: "synthetic"},
		},
		Edges: []domain.Edge{
			{From: "nodeA", To: "nodeB"},
			{From: "nodeB", To: "nodeA"}, // Cycle!
		},
	}
	cyclicBody, _ := json.Marshal(cyclicDef)

	resp, err := http.Post(ts.URL+"/workflows", "application/json", bytes.NewBuffer(cyclicBody))
	if err != nil {
		t.Fatalf("failed to post cyclic workflow: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400 Bad Request for cyclic DAG, got %d", resp.StatusCode)
	}

	// 2. Submit Valid Linear Workflow -> 201 Created
	validDef := domain.WorkflowDefinition{
		ID:   "valid-api-test",
		Name: "Valid Linear Pipeline",
		Nodes: map[string]domain.Node{
			"step1": {ID: "step1", Type: "synthetic"},
			"step2": {ID: "step2", Type: "synthetic"},
		},
		Edges: []domain.Edge{
			{From: "step1", To: "step2"},
		},
	}
	validBody, _ := json.Marshal(validDef)

	resp, err = http.Post(ts.URL+"/workflows", "application/json", bytes.NewBuffer(validBody))
	if err != nil {
		t.Fatalf("failed to post valid workflow: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status 201 Created for valid DAG, got %d", resp.StatusCode)
	}

	var createResp map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&createResp)
	createdWfID := createResp["id"]
	if createdWfID == "" {
		t.Fatalf("expected id in response, got empty")
	}

	// 3. Start Workflow Run -> 201 Created
	resp, err = http.Post(ts.URL+"/workflows/"+createdWfID+"/runs", "application/json", nil)
	if err != nil {
		t.Fatalf("failed to start workflow run: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status 201 Created for run, got %d", resp.StatusCode)
	}

	var runResp map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&runResp)
	runID := runResp["run_id"]
	if runID == "" {
		t.Fatalf("expected run_id in response, got empty")
	}

	// 4. Query Run Status -> 200 OK
	resp, err = http.Get(ts.URL + "/runs/" + runID)
	if err != nil {
		t.Fatalf("failed to get run status: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 for run query, got %d", resp.StatusCode)
	}
}
