package worker

import (
	"context"
	"testing"
	"time"

	"github.com/flowforge/flowforge/internal/outbox"
	"github.com/flowforge/flowforge/internal/repository"
	"github.com/flowforge/flowforge/pkg/domain"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	testDBConnString = "postgres://flowforge:flowforge_password@localhost:5433/flowforge?sslmode=disable"
	testRedisAddr    = "localhost:6379"
)

func setupTestCluster(t *testing.T) (*pgxpool.Pool, *redis.Client) {
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

	return pool, rdb
}

func TestWorker_EndToEndExecution(t *testing.T) {
	pool, rdb := setupTestCluster(t)
	defer pool.Close()
	defer rdb.Close()

	ctx := context.Background()

	// Clear redis streams
	_ = rdb.Del(ctx, outbox.StreamEvents, outbox.StreamTasks).Err()

	repo := repository.NewPostgresRepository(pool)
	relay := outbox.NewRelay(pool, rdb, 10)

	// 1. Create a Workflow with 2 parallel synthetic tasks
	wfID := "22222222-2222-2222-2222-222222222222"
	def := domain.WorkflowDefinition{
		ID:          wfID,
		Name:        "Parallel Synthetic Pipeline",
		Description: "Two parallel synthetic tasks executed by worker fleet",
		Nodes: map[string]domain.Node{
			"taskA": {ID: "taskA", Type: "synthetic", Config: map[string]any{"sleep_ms": 10, "result": `{"msg":"taskA_done"}`}},
			"taskB": {ID: "taskB", Type: "synthetic", Config: map[string]any{"sleep_ms": 10, "result": `{"msg":"taskB_done"}`}},
		},
		Edges:     []domain.Edge{}, // no edges => both are root tasks!
		CreatedAt: time.Now().UTC(),
	}

	if err := repo.CreateWorkflow(ctx, def); err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	// 2. Start Workflow Run (inserts both as READY + creates outbox events)
	runID, err := repo.StartWorkflowRun(ctx, wfID, "trace-worker-test")
	if err != nil {
		t.Fatalf("failed to start workflow run: %v", err)
	}

	// 3. Event Relay pumps outbox events to Redis
	processed, err := relay.ProcessBatch(ctx)
	if err != nil {
		t.Fatalf("relay failed to process batch: %v", err)
	}
	if processed < 3 { // 1 workflow.started + 2 task.ready
		t.Errorf("expected at least 3 outbox events, got %d", processed)
	}

	// 4. Instantiate Worker 1 and Worker 2
	worker1 := NewWorker("worker-1", "test-workers", repo, rdb, nil, 10*time.Second)
	worker2 := NewWorker("worker-2", "test-workers", repo, rdb, nil, 10*time.Second)

	if err := worker1.EnsureConsumerGroup(ctx); err != nil {
		t.Fatalf("failed to create consumer group: %v", err)
	}

	// 5. Workers process tasks
	didWork1, err := worker1.ProcessNextTask(ctx)
	if err != nil || !didWork1 {
		t.Fatalf("worker-1 failed to process task: %v", err)
	}

	didWork2, err := worker2.ProcessNextTask(ctx)
	if err != nil || !didWork2 {
		t.Fatalf("worker-2 failed to process task: %v", err)
	}

	// 6. Verify that both tasks in Postgres are now SUCCEEDED with output saved
	rows, err := pool.Query(ctx, "SELECT node_id, state, output_ref FROM task_runs WHERE workflow_run_id = $1::uuid;", runID)
	if err != nil {
		t.Fatalf("failed to query task runs: %v", err)
	}
	defer rows.Close()

	successCount := 0
	for rows.Next() {
		var nodeID, state, outputRef string
		if err := rows.Scan(&nodeID, &state, &outputRef); err != nil {
			t.Fatalf("scan error: %v", err)
		}
		if state == string(domain.StateSucceeded) {
			successCount++
		}
	}

	if successCount != 2 {
		t.Errorf("expected 2 tasks to SUCCEED, got %d", successCount)
	}
}
