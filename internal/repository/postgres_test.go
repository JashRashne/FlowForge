package repository

import (
	"context"
	"testing"
	"time"

	"github.com/flowforge/flowforge/pkg/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testDBConnString = "postgres://flowforge:flowforge_password@localhost:5433/flowforge?sslmode=disable"

func setupTestDB(t *testing.T) *pgxpool.Pool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, testDBConnString)
	if err != nil {
		t.Skipf("skipping integration test: postgres container unreachable: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping integration test: postgres ping failed: %v", err)
	}

	return pool
}

func createTestWorkflowAndTask(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (string, string) {
	// Create Workflow
	var wfID string
	err := pool.QueryRow(ctx, "INSERT INTO workflows (name) VALUES ('Test Workflow') RETURNING id;").Scan(&wfID)
	if err != nil {
		t.Fatalf("failed to insert test workflow: %v", err)
	}

	// Create Workflow Run
	var runID string
	err = pool.QueryRow(ctx, "INSERT INTO workflow_runs (workflow_id, state) VALUES ($1, 'RUNNING') RETURNING id;", wfID).Scan(&runID)
	if err != nil {
		t.Fatalf("failed to insert test workflow run: %v", err)
	}

	// Create Task Run in READY state
	var taskID string
	err = pool.QueryRow(ctx, "INSERT INTO task_runs (workflow_run_id, node_id, state) VALUES ($1, 'node-1', 'READY') RETURNING id;", runID).Scan(&taskID)
	if err != nil {
		t.Fatalf("failed to insert test task run: %v", err)
	}

	return runID, taskID
}

func TestPostgresRepository_LeaseFencing(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	ctx := context.Background()
	repo := NewPostgresRepository(pool)

	_, taskID := createTestWorkflowAndTask(t, ctx, pool)

	// Step 1: Worker 1 acquires the lease
	taskRun1, err := repo.AcquireLease(ctx, taskID, "worker-1", 10*time.Second)
	if err != nil {
		t.Fatalf("worker-1 failed to acquire lease: %v", err)
	}
	if taskRun1.State != domain.StateLeased {
		t.Errorf("expected state LEASED, got %s", taskRun1.State)
	}
	if taskRun1.LeaseToken == nil || *taskRun1.LeaseToken == "" {
		t.Fatalf("expected valid lease token, got nil")
	}
	tokenWorker1 := *taskRun1.LeaseToken

	// Step 2: Worker 2 attempts to acquire the lease while it is still held
	_, err = repo.AcquireLease(ctx, taskID, "worker-2", 10*time.Second)
	if err != domain.ErrTaskNotLeasable {
		t.Errorf("expected ErrTaskNotLeasable for worker-2, got %v", err)
	}

	// Step 3: Simulate lease expiry and Scheduler resetting task to READY
	_, err = pool.Exec(ctx, "UPDATE task_runs SET state = 'READY' WHERE id = $1;", taskID)
	if err != nil {
		t.Fatalf("failed to simulate lease expiry: %v", err)
	}

	// Step 4: Worker 2 acquires the new lease with a fresh fencing token
	taskRun2, err := repo.AcquireLease(ctx, taskID, "worker-2", 10*time.Second)
	if err != nil {
		t.Fatalf("worker-2 failed to acquire new lease: %v", err)
	}
	tokenWorker2 := *taskRun2.LeaseToken

	if tokenWorker1 == tokenWorker2 {
		t.Fatalf("fencing token collision: token1 and token2 must be unique UUIDs")
	}

	// Step 5: Worker 1 wakes up late and attempts to commit using stale tokenWorker1
	err = repo.CommitTaskSuccess(ctx, taskID, tokenWorker1, "s3://outputs/worker1.json")
	if err != domain.ErrStaleLeaseCommit {
		t.Errorf("expected ErrStaleLeaseCommit for stale worker-1, got %v", err)
	}

	// Step 6: Worker 2 commits with its valid tokenWorker2
	err = repo.CommitTaskSuccess(ctx, taskID, tokenWorker2, "s3://outputs/worker2.json")
	if err != nil {
		t.Fatalf("worker-2 should successfully commit: %v", err)
	}

	// Step 7: Verify final state in database is SUCCEEDED with worker 2's output
	var finalState string
	var finalOutput string
	err = pool.QueryRow(ctx, "SELECT state, output_ref FROM task_runs WHERE id = $1;", taskID).Scan(&finalState, &finalOutput)
	if err != nil {
		t.Fatalf("failed to query final task state: %v", err)
	}

	if finalState != string(domain.StateSucceeded) {
		t.Errorf("expected final state SUCCEEDED, got %s", finalState)
	}
	if finalOutput != "s3://outputs/worker2.json" {
		t.Errorf("expected output to be worker2's output, got %s", finalOutput)
	}
}

func TestPostgresRepository_WorkflowRun_Outbox(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	ctx := context.Background()
	repo := NewPostgresRepository(pool)

	// Define Diamond Workflow: A -> B, A -> C, B -> D, C -> D
	wfID := "11111111-1111-1111-1111-111111111111"
	def := domain.WorkflowDefinition{
		ID:          wfID,
		Name:        "Diamond Data Pipeline",
		Description: "Extract -> Transform(Parallel) -> Load",
		Nodes: map[string]domain.Node{
			"extract":    {ID: "extract", Type: "http", Config: map[string]any{"url": "https://api.example.com/data"}, MaxRetries: 3, TimeoutSec: 30},
			"transform1": {ID: "transform1", Type: "python", Config: map[string]any{"script": "normalize.py"}, MaxRetries: 2, TimeoutSec: 60},
			"transform2": {ID: "transform2", Type: "python", Config: map[string]any{"script": "enrich.py"}, MaxRetries: 2, TimeoutSec: 60},
			"aggregate":  {ID: "aggregate", Type: "synthetic", Config: map[string]any{"operation": "merge"}, MaxRetries: 1, TimeoutSec: 15},
		},
		Edges: []domain.Edge{
			{From: "extract", To: "transform1"},
			{From: "extract", To: "transform2"},
			{From: "transform1", To: "aggregate"},
			{From: "transform2", To: "aggregate"},
		},
		CreatedAt: time.Now().UTC(),
	}

	// 1. Create Workflow in Postgres
	if err := repo.CreateWorkflow(ctx, def); err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	// 2. Fetch Workflow and verify structure
	fetched, err := repo.GetWorkflow(ctx, wfID)
	if err != nil {
		t.Fatalf("failed to get workflow: %v", err)
	}
	if len(fetched.Nodes) != 4 || len(fetched.Edges) != 4 {
		t.Errorf("expected 4 nodes and 4 edges, got %d nodes and %d edges", len(fetched.Nodes), len(fetched.Edges))
	}

	// 3. Start Workflow Run with atomic outbox emission
	traceID := "trace-wf-test-999"
	runID, err := repo.StartWorkflowRun(ctx, wfID, traceID)
	if err != nil {
		t.Fatalf("failed to start workflow run: %v", err)
	}

	// 4. Verify workflow_run record is RUNNING
	var runState string
	err = pool.QueryRow(ctx, "SELECT state FROM workflow_runs WHERE id = $1::uuid;", runID).Scan(&runState)
	if err != nil {
		t.Fatalf("failed to query workflow run: %v", err)
	}
	if runState != "RUNNING" {
		t.Errorf("expected run state RUNNING, got %s", runState)
	}

	// 5. Verify task_runs initial states (extract must be READY, others BLOCKED)
	rows, err := pool.Query(ctx, "SELECT node_id, state FROM task_runs WHERE workflow_run_id = $1::uuid;", runID)
	if err != nil {
		t.Fatalf("failed to query task runs: %v", err)
	}
	defer rows.Close()

	taskStates := make(map[string]string)
	for rows.Next() {
		var nodeID, state string
		if err := rows.Scan(&nodeID, &state); err != nil {
			t.Fatalf("scan error: %v", err)
		}
		taskStates[nodeID] = state
	}

	if taskStates["extract"] != string(domain.StateReady) {
		t.Errorf("expected root task 'extract' to be READY, got %s", taskStates["extract"])
	}
	if taskStates["transform1"] != string(domain.StateBlocked) {
		t.Errorf("expected child task 'transform1' to be BLOCKED, got %s", taskStates["transform1"])
	}
	if taskStates["transform2"] != string(domain.StateBlocked) {
		t.Errorf("expected child task 'transform2' to be BLOCKED, got %s", taskStates["transform2"])
	}
	if taskStates["aggregate"] != string(domain.StateBlocked) {
		t.Errorf("expected child task 'aggregate' to be BLOCKED, got %s", taskStates["aggregate"])
	}

	// 6. Verify transactional outbox events: exactly 2 unsent events (workflow.started and task.ready for extract)
	outboxRows, err := pool.Query(ctx, `
		SELECT event_type, sent_at
		FROM outbox_events
		WHERE workflow_run_id = $1::uuid;
	`, runID)
	if err != nil {
		t.Fatalf("failed to query outbox: %v", err)
	}
	defer outboxRows.Close()

	var eventTypes []string
	for outboxRows.Next() {
		var eventType string
		var sentAt *time.Time
		if err := outboxRows.Scan(&eventType, &sentAt); err != nil {
			t.Fatalf("scan error: %v", err)
		}
		if sentAt != nil {
			t.Errorf("expected initial outbox event sent_at to be NULL, got %v", sentAt)
		}
		eventTypes = append(eventTypes, eventType)
	}

	if len(eventTypes) != 2 {
		t.Fatalf("expected exactly 2 outbox events, got %d (%v)", len(eventTypes), eventTypes)
	}
}
