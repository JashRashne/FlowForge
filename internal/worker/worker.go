package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/flowforge/flowforge/internal/outbox"
	"github.com/flowforge/flowforge/internal/repository"
	"github.com/flowforge/flowforge/pkg/contracts"
	"github.com/flowforge/flowforge/pkg/domain"
	"github.com/redis/go-redis/v9"
)

const (
	DefaultGroupName = "flowforge-workers"
	DefaultLeaseTTL  = 30 * time.Second
)

// Worker represents a disposable, horizontally scalable task execution process.
type Worker struct {
	id            string
	groupName     string
	streamName    string
	repo          *repository.PostgresRepository
	redisClient   *redis.Client
	registry      *AdapterRegistry
	leaseDuration time.Duration
}

// NewWorker initializes a new Worker instance.
func NewWorker(
	id string,
	groupName string,
	repo *repository.PostgresRepository,
	redisClient *redis.Client,
	registry *AdapterRegistry,
	leaseDuration time.Duration,
) *Worker {
	if groupName == "" {
		groupName = DefaultGroupName
	}
	if leaseDuration <= 0 {
		leaseDuration = DefaultLeaseTTL
	}
	if registry == nil {
		registry = NewDefaultRegistry()
	}

	return &Worker{
		id:            id,
		groupName:     groupName,
		streamName:    outbox.StreamTasks,
		repo:          repo,
		redisClient:   redisClient,
		registry:      registry,
		leaseDuration: leaseDuration,
	}
}

// EnsureConsumerGroup creates the Redis Stream consumer group if it doesn't already exist.
func (w *Worker) EnsureConsumerGroup(ctx context.Context) error {
	err := w.redisClient.XGroupCreateMkStream(ctx, w.streamName, w.groupName, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// IsKilled checks if this worker has been paused/killed via chaos injection.
func (w *Worker) IsKilled(ctx context.Context) bool {
	killedKey := fmt.Sprintf("worker:killed:%s", w.id)
	exists, err := w.redisClient.Exists(ctx, killedKey).Result()
	return err == nil && exists > 0
}

// EmitHeartbeat sets an expiring key in Redis to declare worker liveness (TTL: 5 seconds).
func (w *Worker) EmitHeartbeat(ctx context.Context) error {
	if w.IsKilled(ctx) {
		return nil
	}
	key := fmt.Sprintf("worker:%s", w.id)
	return w.redisClient.Set(ctx, key, "alive", 5*time.Second).Err()
}

// ProcessNextTask reads a single message from the Redis stream consumer group,
// attempts atomic lease acquisition, executes the matching adapter, and commits results with fencing.
// Returns (true, nil) if a message was processed, (false, nil) if stream was empty.
func (w *Worker) ProcessNextTask(ctx context.Context) (bool, error) {
	// 1. Read message from consumer group
	streams, err := w.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    w.groupName,
		Consumer: w.id,
		Streams:  []string{w.streamName, ">"},
		Count:    1,
		Block:    1 * time.Second,
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "i/o timeout") {
			return false, nil
		}
		return false, err
	}

	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return false, nil
	}

	msg := streams[0].Messages[0]
	rawPayload, ok := msg.Values["data"].(string)
	if !ok {
		// Malformed message: ACK and discard
		_ = w.redisClient.XAck(ctx, w.streamName, w.groupName, msg.ID).Err()
		return true, nil
	}

	event, err := contracts.FromJSON([]byte(rawPayload))
	if err != nil || event.TaskRunID == nil {
		_ = w.redisClient.XAck(ctx, w.streamName, w.groupName, msg.ID).Err()
		return true, nil
	}

	taskID := *event.TaskRunID

	// 2. Atomic Lease Acquisition in PostgreSQL
	taskRun, err := w.repo.AcquireLease(ctx, taskID, w.id, w.leaseDuration)
	if err != nil {
		if errors.Is(err, domain.ErrTaskNotLeasable) {
			// Another worker won the lease or task was already processed: ACK message and continue
			_ = w.redisClient.XAck(ctx, w.streamName, w.groupName, msg.ID).Err()
			return true, nil
		}
		return false, err
	}

	// 3. Resolve task adapter
	taskType := "synthetic"
	if t, ok := event.Payload["task_type"].(string); ok && t != "" {
		taskType = t
	}

	adapter, exists := w.registry.Get(taskType)
	if !exists {
		adapter, _ = w.registry.Get("synthetic")
	}

	taskConfig := make(map[string]any)
	if cfg, ok := event.Payload["config"].(map[string]any); ok {
		taskConfig = cfg
	}

	// 4. Execute Adapter
	_ = w.EmitHeartbeat(ctx)
	output, execErr := adapter.Execute(ctx, taskConfig)

	// 5. Commit with Fencing Token
	if execErr == nil {
		commitErr := w.repo.CommitTaskSuccess(ctx, taskID, *taskRun.LeaseToken, output)
		if commitErr != nil && !errors.Is(commitErr, domain.ErrStaleLeaseCommit) {
			return false, commitErr
		}
	} else {
		// Calculate retry vs DLQ quarantine
		maxRetries := 2
		if mr, ok := event.Payload["max_retries"].(float64); ok {
			maxRetries = int(mr)
		} else if mr, ok := event.Payload["max_retries"].(int); ok {
			maxRetries = mr
		}

		if taskRun.Attempt < maxRetries {
			// Exponential backoff retry
			backoffSec := 1 << taskRun.Attempt
			if backoffSec > 10 {
				backoffSec = 10
			}
			retryAt := time.Now().Add(time.Duration(backoffSec) * time.Second)
			_ = w.repo.CommitTaskFailure(ctx, taskID, *taskRun.LeaseToken, execErr.Error(), &retryAt, domain.StateRetryWait)
		} else {
			// Exhausted retries -> quarantine task to DLQ
			_ = w.repo.CommitTaskFailure(ctx, taskID, *taskRun.LeaseToken, execErr.Error(), nil, domain.StateDLQ)
		}
	}

	// 6. Acknowledge message in Redis stream
	_ = w.redisClient.XAck(ctx, w.streamName, w.groupName, msg.ID).Err()

	return true, nil
}

// Start launches the continuous worker processing and heartbeat loop.
func (w *Worker) Start(ctx context.Context) error {
	if err := w.EnsureConsumerGroup(ctx); err != nil {
		return err
	}

	heartbeatTicker := time.NewTicker(2 * time.Second)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeatTicker.C:
			_ = w.EmitHeartbeat(ctx)
		default:
			if w.IsKilled(ctx) {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			_, _ = w.ProcessNextTask(ctx)
		}
	}
}
