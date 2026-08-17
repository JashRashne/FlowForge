package outbox

import (
	"context"
	"time"

	"github.com/flowforge/flowforge/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	StreamEvents = "stream:events"
	StreamTasks  = "stream:tasks"
)

// OutboxRecord represents an unsent event in the outbox_events database table.
type OutboxRecord struct {
	ID            string
	EventType     contracts.EventType
	WorkflowRunID string
	TaskRunID     *string
	Payload       []byte
	CreatedAt     time.Time
}

// Relay pumps pending events from the PostgreSQL outbox table to Redis/Valkey Streams.
type Relay struct {
	pool        *pgxpool.Pool
	redisClient *redis.Client
	batchSize   int
}

// NewRelay creates a new Relay instance.
func NewRelay(pool *pgxpool.Pool, redisClient *redis.Client, batchSize int) *Relay {
	if batchSize <= 0 {
		batchSize = 50
	}
	return &Relay{
		pool:        pool,
		redisClient: redisClient,
		batchSize:   batchSize,
	}
}

// FetchUnsent fetches up to 'limit' pending outbox events (where sent_at IS NULL).
func (r *Relay) FetchUnsent(ctx context.Context, limit int) ([]OutboxRecord, error) {
	query := `
		SELECT id, event_type, workflow_run_id, task_run_id, payload, created_at
		FROM outbox_events
		WHERE sent_at IS NULL
		ORDER BY created_at ASC
		LIMIT $1;
	`
	rows, err := r.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []OutboxRecord
	for rows.Next() {
		var rec OutboxRecord
		if err := rows.Scan(&rec.ID, &rec.EventType, &rec.WorkflowRunID, &rec.TaskRunID, &rec.Payload, &rec.CreatedAt); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

// MarkSent flags an outbox event as successfully delivered to the stream.
func (r *Relay) MarkSent(ctx context.Context, eventID string) error {
	query := `
		UPDATE outbox_events
		SET sent_at = NOW()
		WHERE id = $1::uuid;
	`
	_, err := r.pool.Exec(ctx, query, eventID)
	return err
}

// ProcessBatch fetches unsent events, publishes each to Redis Streams,
// and marks them as sent in PostgreSQL.
//
// Rules to implement:
//  1. Call r.FetchUnsent(ctx, r.batchSize). If empty or err != nil, return.
//  2. For every record:
//     a) Publish to Redis stream "stream:events" with values: map[string]any{"data": string(record.Payload)}
//     using r.redisClient.XAdd(ctx, &redis.XAddArgs{ Stream: StreamEvents, Values: ... })
//     b) If record.EventType == contracts.EventTaskReady, ALSO publish to "stream:tasks" with the same payload!
//     c) Call r.MarkSent(ctx, record.ID).
//  3. Return count of processed records.
func (r *Relay) ProcessBatch(ctx context.Context) (int, error) {
	records, err := r.FetchUnsent(ctx, r.batchSize)
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, nil
	}

	processed := 0

	for _, record := range records {
		err := r.redisClient.XAdd(ctx, &redis.XAddArgs{
			Stream: StreamEvents, // "stream:events"
			Values: map[string]any{
				"data": string(record.Payload),
			},
		}).Err()
		if err != nil {
			return processed, err
		}

		if record.EventType == contracts.EventTaskReady {
			err := r.redisClient.XAdd(ctx, &redis.XAddArgs{
				Stream: StreamTasks, // "stream:tasks"
				Values: map[string]any{
					"data": string(record.Payload),
				},
			}).Err()
			if err != nil {
				return processed, err
			}
		}

		err = r.MarkSent(ctx, record.ID)
		if err != nil {
			return processed, err
		}

		processed++
	}

	return processed, nil
}

// Start runs the background relay polling loop with a ticker until ctx is cancelled.
func (r *Relay) Start(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Process batches until the queue is drained
			for {
				n, err := r.ProcessBatch(ctx)
				if err != nil || n == 0 {
					break
				}
			}
		}
	}
}
