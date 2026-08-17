package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/flowforge/flowforge/internal/repository"
	"github.com/flowforge/flowforge/pkg/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testDBConnString = "postgres://flowforge:flowforge_password@localhost:5433/flowforge?sslmode=disable"

func setupTestDB(t *testing.T) *pgxpool.Pool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, testDBConnString)
	if err != nil {
		t.Skipf("skipping test: postgres unreachable: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping test: postgres ping failed: %v", err)
	}
	return pool
}

func TestScheduler_UnlockBlockedTasks(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	ctx := context.Background()
	repo := repository.NewPostgresRepository(pool)
	sched := NewScheduler(pool, repo)

	// 1. Create a Linear DAG: A -> B
	wfID := "33333333-3333-3333-3333-333333333333"
	def := domain.WorkflowDefinition{
		ID:          wfID,
		Name:        "Linear Pipeline",
		Description: "A -> B pipeline",
		Nodes: map[string]domain.Node{
			"taskA": {ID: "taskA", Type: "synthetic", Config: map[string]any{}},
			"taskB": {ID: "taskB", Type: "synthetic", Config: map[string]any{}},
		},
		Edges: []domain.Edge{
			{From: "taskA", To: "taskB"},
		},
		CreatedAt: time.Now().UTC(),
	}

	if err := repo.CreateWorkflow(ctx, def); err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	// 2. Start Workflow Run (A is READY, B is BLOCKED)
	runID, err := repo.StartWorkflowRun(ctx, wfID, "trace-sched-test")
	if err != nil {
		t.Fatalf("failed to start workflow run: %v", err)
	}

	// 3. Run UnlockBlockedTasks while A is still not SUCCEEDED -> should unlock 0
	unlocked, err := sched.UnlockBlockedTasks(ctx)
	if err != nil {
		t.Fatalf("UnlockBlockedTasks failed: %v", err)
	}
	if unlocked != 0 {
		t.Errorf("expected 0 unlocked tasks before parent finishes, got %d", unlocked)
	}

	// 4. Mark taskA as SUCCEEDED
	_, err = pool.Exec(ctx, "UPDATE task_runs SET state = 'SUCCEEDED' WHERE workflow_run_id = $1::uuid AND node_id = 'taskA';", runID)
	if err != nil {
		t.Fatalf("failed to update taskA to SUCCEEDED: %v", err)
	}

	// 5. Run UnlockBlockedTasks -> taskB should now UNLOCK to READY!
	unlocked, err = sched.UnlockBlockedTasks(ctx)
	if err != nil {
		t.Fatalf("UnlockBlockedTasks failed: %v", err)
	}
	if unlocked != 1 {
		t.Errorf("expected exactly 1 unlocked task (taskB), got %d", unlocked)
	}

	// 6. Verify taskB in DB is now in READY state
	var stateB string
	err = pool.QueryRow(ctx, "SELECT state FROM task_runs WHERE workflow_run_id = $1::uuid AND node_id = 'taskB';", runID).Scan(&stateB)
	if err != nil {
		t.Fatalf("failed to query taskB state: %v", err)
	}
	if stateB != string(domain.StateReady) {
		t.Errorf("expected taskB to be READY, got %s", stateB)
	}
}

func TestScheduler_RecoverExpiredLeases(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	ctx := context.Background()
	repo := repository.NewPostgresRepository(pool)
	sched := NewScheduler(pool, repo)

	// Create workflow & run
	wfID := "44444444-4444-4444-4444-444444444444"
	def := domain.WorkflowDefinition{
		ID:   wfID,
		Name: "Crash Recovery Pipeline",
		Nodes: map[string]domain.Node{
			"crashTask": {ID: "crashTask", Type: "synthetic"},
		},
		Edges:     []domain.Edge{},
		CreatedAt: time.Now().UTC(),
	}

	if err := repo.CreateWorkflow(ctx, def); err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	runID, err := repo.StartWorkflowRun(ctx, wfID, "trace-crash-test")
	if err != nil {
		t.Fatalf("failed to start workflow run: %v", err)
	}

	// Simulate Worker acquiring task and then crashing past lease duration (lease_expires_at in past)
	_, err = pool.Exec(ctx, `
		UPDATE task_runs
		SET state = 'RUNNING',
		    lease_owner = 'crashed-worker-99',
		    lease_token = gen_random_uuid(),
		    lease_expires_at = NOW() - INTERVAL '10 seconds'
		WHERE workflow_run_id = $1::uuid AND node_id = 'crashTask';
	`, runID)
	if err != nil {
		t.Fatalf("failed to simulate crashed worker: %v", err)
	}

	// Run RecoverExpiredLeases
	recovered, err := sched.RecoverExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("RecoverExpiredLeases failed: %v", err)
	}
	if recovered != 1 {
		t.Errorf("expected 1 recovered task, got %d", recovered)
	}

	// Verify task is back in READY with incremented attempt
	var state, leaseOwner string
	var attempt int
	err = pool.QueryRow(ctx, "SELECT state, COALESCE(lease_owner, 'NONE'), attempt FROM task_runs WHERE workflow_run_id = $1::uuid AND node_id = 'crashTask';", runID).Scan(&state, &leaseOwner, &attempt)
	if err != nil {
		t.Fatalf("failed to query recovered task: %v", err)
	}

	if state != string(domain.StateReady) {
		t.Errorf("expected recovered state READY, got %s", state)
	}
	if leaseOwner != "NONE" {
		t.Errorf("expected lease_owner to be cleared, got %s", leaseOwner)
	}
	if attempt != 1 {
		t.Errorf("expected attempt count to be 1, got %d", attempt)
	}
}

func TestScheduler_CheckWorkflowCompletion(t *testing.T) {
	pool := setupTestDB(t)
	defer pool.Close()

	ctx := context.Background()
	repo := repository.NewPostgresRepository(pool)
	sched := NewScheduler(pool, repo)

	wfID := "55555555-5555-5555-5555-555555555555"
	def := domain.WorkflowDefinition{
		ID:   wfID,
		Name: "Completion Pipeline",
		Nodes: map[string]domain.Node{
			"singleTask": {ID: "singleTask", Type: "synthetic"},
		},
		Edges:     []domain.Edge{},
		CreatedAt: time.Now().UTC(),
	}

	_ = repo.CreateWorkflow(ctx, def)
	runID, _ := repo.StartWorkflowRun(ctx, wfID, "trace-completion-test")

	// Mark the task as SUCCEEDED
	_, _ = pool.Exec(ctx, "UPDATE task_runs SET state = 'SUCCEEDED' WHERE workflow_run_id = $1::uuid;", runID)

	// Run CheckWorkflowCompletion
	completed, err := sched.CheckWorkflowCompletion(ctx)
	if err != nil {
		t.Fatalf("CheckWorkflowCompletion failed: %v", err)
	}
	if completed < 1 {
		t.Errorf("expected at least 1 completed workflow run, got %d", completed)
	}

	// Verify our specific workflow_runs state is SUCCEEDED
	var runState string
	err = pool.QueryRow(ctx, "SELECT state FROM workflow_runs WHERE id = $1::uuid;", runID).Scan(&runState)
	if err != nil {
		t.Fatalf("failed to query workflow run state: %v", err)
	}
	if runState != "SUCCEEDED" {
		t.Errorf("expected workflow_run state SUCCEEDED, got %s", runState)
	}
}
