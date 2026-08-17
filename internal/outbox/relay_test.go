package outbox

import (
	"context"
	"testing"
	"time"

	"github.com/flowforge/flowforge/pkg/contracts"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	testDBConnString = "postgres://flowforge:flowforge_password@localhost:5433/flowforge?sslmode=disable"
	testRedisAddr    = "localhost:6379"
)

func setupTestDBAndRedis(t *testing.T) (*pgxpool.Pool, *redis.Client) {
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

func TestRelay_ProcessBatch(t *testing.T) {
	pool, rdb := setupTestDBAndRedis(t)
	defer pool.Close()
	defer rdb.Close()

	ctx := context.Background()

	// Clear streams for clean test
	_ = rdb.Del(ctx, StreamEvents, StreamTasks).Err()

	// 1. Insert 2 test outbox events (1 workflow.started, 1 task.ready)
	wfRunID := uuid.NewString()
	taskRunID := uuid.NewString()

	event1 := contracts.NewEvent(contracts.EventWorkflowStarted, wfRunID, nil, 0, "trace-1", map[string]any{"status": "starting"})
	data1, _ := event1.ToJSON()

	event2 := contracts.NewEvent(contracts.EventTaskReady, wfRunID, &taskRunID, 1, "trace-1", map[string]any{"task_type": "http"})
	data2, _ := event2.ToJSON()

	_, err := pool.Exec(ctx, `
		INSERT INTO outbox_events (id, event_type, workflow_run_id, payload, created_at, sent_at)
		VALUES ($1::uuid, $2, $3::uuid, $4::jsonb, NOW(), NULL);
	`, event1.EventID, string(event1.EventType), wfRunID, string(data1))
	if err != nil {
		t.Fatalf("failed to insert event1: %v", err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO outbox_events (id, event_type, workflow_run_id, task_run_id, payload, created_at, sent_at)
		VALUES ($1::uuid, $2, $3::uuid, $4::uuid, $5::jsonb, NOW(), NULL);
	`, event2.EventID, string(event2.EventType), wfRunID, taskRunID, string(data2))
	if err != nil {
		t.Fatalf("failed to insert event2: %v", err)
	}

	// 2. Run ProcessBatch
	relay := NewRelay(pool, rdb, 10)
	processedCount, err := relay.ProcessBatch(ctx)
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if processedCount < 2 {
		t.Errorf("expected at least 2 processed events, got %d", processedCount)
	}

	// 3. Verify outbox events are marked as sent_at != NULL in Postgres
	var unsentCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE id IN ($1::uuid, $2::uuid) AND sent_at IS NULL;", event1.EventID, event2.EventID).Scan(&unsentCount)
	if err != nil {
		t.Fatalf("failed to count unsent events: %v", err)
	}
	if unsentCount != 0 {
		t.Errorf("expected 0 unsent events, got %d", unsentCount)
	}

	// 4. Verify stream:events received messages
	eventsStream, err := rdb.XRange(ctx, StreamEvents, "-", "+").Result()
	if err != nil {
		t.Fatalf("failed to read from stream:events: %v", err)
	}
	if len(eventsStream) < 2 {
		t.Errorf("expected at least 2 messages in stream:events, got %d", len(eventsStream))
	}

	// 5. Verify stream:tasks received the task.ready event
	tasksStream, err := rdb.XRange(ctx, StreamTasks, "-", "+").Result()
	if err != nil {
		t.Fatalf("failed to read from stream:tasks: %v", err)
	}
	if len(tasksStream) < 1 {
		t.Errorf("expected at least 1 message in stream:tasks, got %d", len(tasksStream))
	}
}
