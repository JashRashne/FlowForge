package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/flowforge/flowforge/internal/dag"
	"github.com/flowforge/flowforge/pkg/contracts"
	"github.com/flowforge/flowforge/pkg/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresRepository handles durable database operations with lease fencing and transactions.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresRepository initializes a new PostgreSQL repository with an active connection pool.
func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

// AcquireLease attempts to atomically transition a task from READY to LEASED.
// It assigns a fresh UUID lease_token (the fencing token) and an expiration timestamp.
// If the task was already leased or is not in READY state, it returns ErrTaskNotLeasable.
func (r *PostgresRepository) AcquireLease(
	ctx context.Context,
	taskID string,
	workerID string,
	leaseDuration time.Duration,
) (*domain.TaskRun, error) {
	newToken := uuid.NewString()
	durationSec := int(leaseDuration.Seconds())
	if durationSec <= 0 {
		durationSec = 30 // default 30s lease
	}

	query := `
		UPDATE task_runs
		SET state = 'LEASED',
		    lease_owner = $1,
		    lease_token = $2::uuid,
		    lease_expires_at = NOW() + ($3 * INTERVAL '1 second'),
		    started_at = COALESCE(started_at, NOW()),
		    version = version + 1
		WHERE id = $4::uuid
		  AND state = 'READY'
		RETURNING id, workflow_run_id, node_id, state, attempt, lease_owner, lease_token, lease_expires_at, retry_at, output_ref, created_at, started_at, finished_at, version;
	`

	var tr domain.TaskRun
	var leaseTokenUUID *string
	var leaseOwner *string

	err := r.pool.QueryRow(ctx, query, workerID, newToken, durationSec, taskID).Scan(
		&tr.ID,
		&tr.WorkflowRunID,
		&tr.NodeID,
		&tr.State,
		&tr.Attempt,
		&leaseOwner,
		&leaseTokenUUID,
		&tr.LeaseExpiresAt,
		&tr.RetryAt,
		&tr.OutputRef,
		&tr.CreatedAt,
		&tr.StartedAt,
		&tr.FinishedAt,
		&tr.Version,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrTaskNotLeasable
		}
		return nil, err
	}

	tr.LeaseOwner = leaseOwner
	tr.LeaseToken = leaseTokenUUID

	return &tr, nil
}

// CommitTaskSuccess performs a fenced update to transition a task to SUCCEEDED.
// If the worker's lease_token no longer matches the database (e.g. lease expired and
// was acquired by a newer worker), RowsAffected will be 0, returning ErrStaleLeaseCommit.
func (r *PostgresRepository) CommitTaskSuccess(
	ctx context.Context,
	taskID string,
	leaseToken string,
	outputRef string,
) error {
	query := `
		UPDATE task_runs
		SET state = 'SUCCEEDED',
		    output_ref = $1,
		    finished_at = NOW(),
		    version = version + 1
		WHERE id = $2::uuid
		  AND state IN ('LEASED', 'RUNNING')
		  AND lease_token = $3::uuid;
	`

	cmdTag, err := r.pool.Exec(ctx, query, outputRef, taskID, leaseToken)
	if err != nil {
		return err
	}

	if cmdTag.RowsAffected() == 0 {
		return domain.ErrStaleLeaseCommit
	}

	return nil
}

// CommitTaskFailure transitions a task to RETRY_WAIT (if retryAt is set) or FAILED/DLQ.
// It also enforces lease token fencing so stale workers cannot fail a reassigned task.
func (r *PostgresRepository) CommitTaskFailure(
	ctx context.Context,
	taskID string,
	leaseToken string,
	errorMessage string,
	retryAt *time.Time,
	targetState domain.TaskState,
) error {
	errJSON, _ := json.Marshal(map[string]string{"message": errorMessage})

	query := `
		UPDATE task_runs
		SET state = $1,
		    error = $2::jsonb,
		    retry_at = $3,
		    finished_at = CASE WHEN $1 IN ('FAILED', 'DLQ') THEN NOW() ELSE NULL END,
		    version = version + 1
		WHERE id = $4::uuid
		  AND state IN ('LEASED', 'RUNNING')
		  AND lease_token = $5::uuid;
	`

	cmdTag, err := r.pool.Exec(ctx, query, string(targetState), string(errJSON), retryAt, taskID, leaseToken)
	if err != nil {
		return err
	}

	if cmdTag.RowsAffected() == 0 {
		return domain.ErrStaleLeaseCommit
	}

	return nil
}

// CreateWorkflow persists a static DAG definition (workflow metadata, nodes, and edges) in a single transaction.
func (r *PostgresRepository) CreateWorkflow(ctx context.Context, def domain.WorkflowDefinition) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 1. Insert workflow metadata
	_, err = tx.Exec(ctx, `
		INSERT INTO workflows (id, name, description, created_at)
		VALUES ($1::uuid, $2, $3, COALESCE($4, NOW()))
		ON CONFLICT (id) DO NOTHING;
	`, def.ID, def.Name, def.Description, def.CreatedAt)
	if err != nil {
		return err
	}

	// 2. Insert static nodes
	for _, node := range def.Nodes {
		configJSON, err := json.Marshal(node.Config)
		if err != nil {
			configJSON = []byte("{}")
		}
		maxRetries := node.MaxRetries
		if maxRetries <= 0 {
			maxRetries = 3
		}
		timeoutSec := node.TimeoutSec
		if timeoutSec <= 0 {
			timeoutSec = 60
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO workflow_nodes (workflow_id, node_id, task_type, config, max_retries, timeout_sec)
			VALUES ($1::uuid, $2, $3, $4::jsonb, $5, $6)
			ON CONFLICT (workflow_id, node_id) DO UPDATE
			SET task_type = EXCLUDED.task_type,
			    config = EXCLUDED.config,
			    max_retries = EXCLUDED.max_retries,
			    timeout_sec = EXCLUDED.timeout_sec;
		`, def.ID, node.ID, node.Type, string(configJSON), maxRetries, timeoutSec)
		if err != nil {
			return err
		}
	}

	// 3. Insert static edges
	for _, edge := range def.Edges {
		_, err = tx.Exec(ctx, `
			INSERT INTO workflow_edges (workflow_id, from_node, to_node)
			VALUES ($1::uuid, $2, $3)
			ON CONFLICT (workflow_id, from_node, to_node) DO NOTHING;
		`, def.ID, edge.From, edge.To)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// GetWorkflow fetches a full workflow definition including its nodes and edges.
func (r *PostgresRepository) GetWorkflow(ctx context.Context, workflowID string) (*domain.WorkflowDefinition, error) {
	var def domain.WorkflowDefinition
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, description, created_at
		FROM workflows
		WHERE id = $1::uuid;
	`, workflowID).Scan(&def.ID, &def.Name, &def.Description, &def.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNodeNotFound
		}
		return nil, err
	}

	def.Nodes = make(map[string]domain.Node)
	nodeRows, err := r.pool.Query(ctx, `
		SELECT node_id, task_type, config, max_retries, timeout_sec
		FROM workflow_nodes
		WHERE workflow_id = $1::uuid;
	`, workflowID)
	if err != nil {
		return nil, err
	}
	defer nodeRows.Close()

	for nodeRows.Next() {
		var n domain.Node
		var configBytes []byte
		if err := nodeRows.Scan(&n.ID, &n.Type, &configBytes, &n.MaxRetries, &n.TimeoutSec); err != nil {
			return nil, err
		}
		if len(configBytes) > 0 {
			_ = json.Unmarshal(configBytes, &n.Config)
		}
		def.Nodes[n.ID] = n
	}

	edgeRows, err := r.pool.Query(ctx, `
		SELECT from_node, to_node
		FROM workflow_edges
		WHERE workflow_id = $1::uuid;
	`, workflowID)
	if err != nil {
		return nil, err
	}
	defer edgeRows.Close()

	for edgeRows.Next() {
		var e domain.Edge
		if err := edgeRows.Scan(&e.From, &e.To); err != nil {
			return nil, err
		}
		def.Edges = append(def.Edges, e)
	}

	return &def, nil
}

// StartWorkflowRun validates DAG acyclicity, computes initial root tasks (InDegree == 0),
// and atomically inserts the workflow run, task runs (READY for roots, BLOCKED for children),
// and initial outbox events in a single PostgreSQL transaction.
func (r *PostgresRepository) StartWorkflowRun(
	ctx context.Context,
	workflowID string,
	traceID string,
) (string, error) {
	wfDef, err := r.GetWorkflow(ctx, workflowID)
	if err != nil {
		return "", err
	}

	// 1. Build and validate in-memory DAG
	graph, err := dag.BuildGraph(*wfDef)
	if err != nil {
		return "", err
	}
	if _, err := graph.TopologicalSort(); err != nil {
		return "", err
	}

	roots := graph.GetRoots()
	rootSet := make(map[string]bool)
	for _, root := range roots {
		rootSet[root] = true
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	// 2. Insert workflow_runs record
	runID := uuid.NewString()
	_, err = tx.Exec(ctx, `
		INSERT INTO workflow_runs (id, workflow_id, state, started_at)
		VALUES ($1::uuid, $2::uuid, 'RUNNING', NOW());
	`, runID, workflowID)
	if err != nil {
		return "", err
	}

	// 3. Insert task_runs for all nodes
	taskIDMap := make(map[string]string)
	for nodeID := range wfDef.Nodes {
		taskID := uuid.NewString()
		taskIDMap[nodeID] = taskID

		initialState := domain.StateBlocked
		if rootSet[nodeID] {
			initialState = domain.StateReady
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO task_runs (id, workflow_run_id, node_id, state, attempt, created_at, version)
			VALUES ($1::uuid, $2::uuid, $3, $4, 0, NOW(), 0);
		`, taskID, runID, nodeID, string(initialState))
		if err != nil {
			return "", err
		}
	}

	// 4. Emit transactional outbox events:
	// a) workflow.started event
	wfStartedEvent := contracts.NewEvent(
		contracts.EventWorkflowStarted,
		runID,
		nil,
		0,
		traceID,
		map[string]any{
			"workflow_id": workflowID,
			"node_count":  len(wfDef.Nodes),
		},
	)
	wfEventPayload, _ := wfStartedEvent.ToJSON()

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (id, event_type, workflow_run_id, payload, created_at, sent_at)
		VALUES ($1::uuid, $2, $3::uuid, $4::jsonb, NOW(), NULL);
	`, wfStartedEvent.EventID, string(wfStartedEvent.EventType), runID, string(wfEventPayload))
	if err != nil {
		return "", err
	}

	// b) task.ready event for each root node
	for _, rootNodeID := range roots {
		taskID := taskIDMap[rootNodeID]
		nodeDef := wfDef.Nodes[rootNodeID]

		taskReadyEvent := contracts.NewEvent(
			contracts.EventTaskReady,
			runID,
			&taskID,
			1,
			traceID,
			map[string]any{
				"node_id":   rootNodeID,
				"task_type": nodeDef.Type,
				"config":    nodeDef.Config,
			},
		)
		taskPayload, _ := taskReadyEvent.ToJSON()

		_, err = tx.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, workflow_run_id, task_run_id, payload, created_at, sent_at)
			VALUES ($1::uuid, $2, $3::uuid, $4::uuid, $5::jsonb, NOW(), NULL);
		`, taskReadyEvent.EventID, string(taskReadyEvent.EventType), runID, taskID, string(taskPayload))
		if err != nil {
			return "", err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return "", err
	}

	return runID, nil
}
