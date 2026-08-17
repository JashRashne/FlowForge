package repository

import (
	"context"
	"errors"
	"time"

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
