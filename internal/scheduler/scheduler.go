package scheduler

import (
	"context"
	"encoding/json"
	"time"

	"github.com/flowforge/flowforge/internal/repository"
	"github.com/flowforge/flowforge/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Scheduler coordinates DAG progression, lease expiration crash recovery, and retry promotions.
type Scheduler struct {
	pool *pgxpool.Pool
	repo *repository.PostgresRepository
}

// NewScheduler creates a new Scheduler instance.
func NewScheduler(pool *pgxpool.Pool, repo *repository.PostgresRepository) *Scheduler {
	return &Scheduler{
		pool: pool,
		repo: repo,
	}
}

// UnlockBlockedTasks finds BLOCKED tasks whose required predecessor parents have all SUCCEEDED,
// transitions them to READY, and atomically writes task.ready outbox events in a single transaction.
func (s *Scheduler) UnlockBlockedTasks(ctx context.Context) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	// CTE query: Finds BLOCKED tasks in active workflows that have ZERO non-succeeded parents
	query := `
		WITH ready_candidates AS (
			SELECT tr.id AS task_run_id, tr.workflow_run_id, tr.node_id, wn.task_type, wn.config, wn.max_retries, wr.workflow_id
			FROM task_runs tr
			JOIN workflow_runs wr ON tr.workflow_run_id = wr.id
			JOIN workflow_nodes wn ON wr.workflow_id = wn.workflow_id AND tr.node_id = wn.node_id
			WHERE tr.state = 'BLOCKED'
			  AND wr.state = 'RUNNING'
			  AND NOT EXISTS (
				  SELECT 1
				  FROM workflow_edges we
				  JOIN task_runs parent_tr ON parent_tr.workflow_run_id = tr.workflow_run_id AND parent_tr.node_id = we.from_node
				  WHERE we.workflow_id = wr.workflow_id
				    AND we.to_node = tr.node_id
				    AND parent_tr.state != 'SUCCEEDED'
			  )
		)
		UPDATE task_runs
		SET state = 'READY',
		    version = task_runs.version + 1
		FROM ready_candidates
		WHERE task_runs.id = ready_candidates.task_run_id
		RETURNING task_runs.id, task_runs.workflow_run_id, ready_candidates.node_id, ready_candidates.task_type, ready_candidates.config, ready_candidates.max_retries;
	`

	rows, err := tx.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	type unlockedTask struct {
		taskRunID     string
		workflowRunID string
		nodeID        string
		taskType      string
		config        map[string]any
		maxRetries    int
	}

	var unlocked []unlockedTask
	for rows.Next() {
		var u unlockedTask
		var configBytes []byte
		if err := rows.Scan(&u.taskRunID, &u.workflowRunID, &u.nodeID, &u.taskType, &configBytes, &u.maxRetries); err != nil {
			return 0, err
		}
		if len(configBytes) > 0 {
			_ = json.Unmarshal(configBytes, &u.config)
		}
		unlocked = append(unlocked, u)
	}
	rows.Close()

	// Insert task.ready outbox events for all newly unlocked tasks
	for _, u := range unlocked {
		event := contracts.NewEvent(
			contracts.EventTaskReady,
			u.workflowRunID,
			&u.taskRunID,
			0,
			"",
			map[string]any{
				"node_id":     u.nodeID,
				"task_type":   u.taskType,
				"config":      u.config,
				"max_retries": u.maxRetries,
			},
		)
		payloadBytes, _ := event.ToJSON()

		_, err := tx.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, workflow_run_id, task_run_id, payload, created_at, sent_at)
			VALUES ($1::uuid, $2, $3::uuid, $4::uuid, $5::jsonb, NOW(), NULL);
		`, event.EventID, string(event.EventType), u.workflowRunID, u.taskRunID, string(payloadBytes))
		if err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	return len(unlocked), nil
}

// RecoverExpiredLeases detects tasks held by crashed workers (lease_expires_at < NOW()),
// resets them to READY with incremented attempt counts, and writes outbox events so another worker picks them up.
func (s *Scheduler) RecoverExpiredLeases(ctx context.Context) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	query := `
		WITH expired AS (
			SELECT tr.id AS task_run_id, tr.workflow_run_id, tr.node_id, tr.lease_owner, wn.task_type, wn.config, wn.max_retries
			FROM task_runs tr
			JOIN workflow_runs wr ON tr.workflow_run_id = wr.id
			JOIN workflow_nodes wn ON wr.workflow_id = wn.workflow_id AND tr.node_id = wn.node_id
			WHERE tr.state IN ('LEASED', 'RUNNING')
			  AND tr.lease_expires_at < NOW()
			  AND wr.state = 'RUNNING'
		)
		UPDATE task_runs
		SET state = 'READY',
		    lease_owner = NULL,
		    lease_token = NULL,
		    lease_expires_at = NULL,
		    attempt = task_runs.attempt + 1,
		    version = task_runs.version + 1
		FROM expired
		WHERE task_runs.id = expired.task_run_id
		RETURNING task_runs.id, task_runs.workflow_run_id, expired.node_id, expired.lease_owner, expired.task_type, expired.config, expired.max_retries, task_runs.attempt;
	`

	rows, err := tx.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	type recoveredTask struct {
		taskRunID     string
		workflowRunID string
		nodeID        string
		previousOwner *string
		taskType      string
		config        map[string]any
		maxRetries    int
		attempt       int
	}

	var recovered []recoveredTask
	for rows.Next() {
		var r recoveredTask
		var configBytes []byte
		if err := rows.Scan(&r.taskRunID, &r.workflowRunID, &r.nodeID, &r.previousOwner, &r.taskType, &configBytes, &r.maxRetries, &r.attempt); err != nil {
			return 0, err
		}
		if len(configBytes) > 0 {
			_ = json.Unmarshal(configBytes, &r.config)
		}
		recovered = append(recovered, r)
	}
	rows.Close()

	for _, r := range recovered {
		// Emit task.ready event for reassignment
		event := contracts.NewEvent(
			contracts.EventTaskReady,
			r.workflowRunID,
			&r.taskRunID,
			r.attempt,
			"",
			map[string]any{
				"node_id":     r.nodeID,
				"task_type":   r.taskType,
				"config":      r.config,
				"max_retries": r.maxRetries,
				"recovered":   true,
			},
		)
		payloadBytes, _ := event.ToJSON()

		_, err := tx.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, workflow_run_id, task_run_id, payload, created_at, sent_at)
			VALUES ($1::uuid, $2, $3::uuid, $4::uuid, $5::jsonb, NOW(), NULL);
		`, event.EventID, string(event.EventType), r.workflowRunID, r.taskRunID, string(payloadBytes))
		if err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	return len(recovered), nil
}

// AdvanceRetries transitions RETRY_WAIT tasks whose backoff timer (retry_at <= NOW()) has elapsed back to READY.
func (s *Scheduler) AdvanceRetries(ctx context.Context) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	query := `
		WITH retryable AS (
			SELECT tr.id AS task_run_id, tr.workflow_run_id, tr.node_id, wn.task_type, wn.config, wn.max_retries, tr.attempt
			FROM task_runs tr
			JOIN workflow_runs wr ON tr.workflow_run_id = wr.id
			JOIN workflow_nodes wn ON wr.workflow_id = wn.workflow_id AND tr.node_id = wn.node_id
			WHERE tr.state = 'RETRY_WAIT'
			  AND tr.retry_at <= NOW()
			  AND wr.state = 'RUNNING'
		)
		UPDATE task_runs
		SET state = 'READY',
		    retry_at = NULL,
		    attempt = task_runs.attempt + 1,
		    version = task_runs.version + 1
		FROM retryable
		WHERE task_runs.id = retryable.task_run_id
		RETURNING task_runs.id, task_runs.workflow_run_id, retryable.node_id, retryable.task_type, retryable.config, retryable.max_retries, task_runs.attempt;
	`

	rows, err := tx.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	type retryTask struct {
		taskRunID     string
		workflowRunID string
		nodeID        string
		taskType      string
		config        map[string]any
		maxRetries    int
		attempt       int
	}

	var retried []retryTask
	for rows.Next() {
		var r retryTask
		var configBytes []byte
		if err := rows.Scan(&r.taskRunID, &r.workflowRunID, &r.nodeID, &r.taskType, &configBytes, &r.maxRetries, &r.attempt); err != nil {
			return 0, err
		}
		if len(configBytes) > 0 {
			_ = json.Unmarshal(configBytes, &r.config)
		}
		retried = append(retried, r)
	}
	rows.Close()

	for _, r := range retried {
		event := contracts.NewEvent(
			contracts.EventTaskReady,
			r.workflowRunID,
			&r.taskRunID,
			r.attempt,
			"",
			map[string]any{
				"node_id":     r.nodeID,
				"task_type":   r.taskType,
				"config":      r.config,
				"max_retries": r.maxRetries,
				"retried":     true,
			},
		)
		payloadBytes, _ := event.ToJSON()

		_, err := tx.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, workflow_run_id, task_run_id, payload, created_at, sent_at)
			VALUES ($1::uuid, $2, $3::uuid, $4::uuid, $5::jsonb, NOW(), NULL);
		`, event.EventID, string(event.EventType), r.workflowRunID, r.taskRunID, string(payloadBytes))
		if err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	return len(retried), nil
}

// CheckWorkflowCompletion inspects active workflow runs to determine if they reached terminal states (SUCCEEDED or FAILED).
func (s *Scheduler) CheckWorkflowCompletion(ctx context.Context) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	// Check workflows where all tasks SUCCEEDED
	succeededQuery := `
		UPDATE workflow_runs
		SET state = 'SUCCEEDED',
		    finished_at = NOW()
		WHERE state = 'RUNNING'
		  AND NOT EXISTS (
			  SELECT 1 FROM task_runs
			  WHERE task_runs.workflow_run_id = workflow_runs.id
			    AND task_runs.state != 'SUCCEEDED'
		  )
		RETURNING id;
	`
	rows, err := tx.Query(ctx, succeededQuery)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var completedRunIDs []string
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			return 0, err
		}
		completedRunIDs = append(completedRunIDs, runID)
	}
	rows.Close()

	for _, runID := range completedRunIDs {
		event := contracts.NewEvent(
			contracts.EventWorkflowSucceeded,
			runID,
			nil,
			0,
			"",
			map[string]any{"status": "SUCCEEDED"},
		)
		payloadBytes, _ := event.ToJSON()

		_, err := tx.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, workflow_run_id, payload, created_at, sent_at)
			VALUES ($1::uuid, $2, $3::uuid, $4::jsonb, NOW(), NULL);
		`, event.EventID, string(event.EventType), runID, string(payloadBytes))
		if err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	return len(completedRunIDs), nil
}

// RunOnce executes a single scheduling iteration.
func (s *Scheduler) RunOnce(ctx context.Context) error {
	if _, err := s.UnlockBlockedTasks(ctx); err != nil {
		return err
	}
	if _, err := s.RecoverExpiredLeases(ctx); err != nil {
		return err
	}
	if _, err := s.AdvanceRetries(ctx); err != nil {
		return err
	}
	if _, err := s.CheckWorkflowCompletion(ctx); err != nil {
		return err
	}
	return nil
}

// Start launches the continuous scheduler ticker loop.
func (s *Scheduler) Start(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_ = s.RunOnce(ctx)
		}
	}
}
