package domain

import (
	"errors"
	"time"
)

// TaskState represents the lifecycle status of a task run in the state machine.
type TaskState string

const (
	// StateBlocked: Task has upstream dependencies that have not yet succeeded.
	StateBlocked TaskState = "BLOCKED"
	// StateReady: All dependencies have succeeded; task is eligible to be picked up by a worker.
	StateReady TaskState = "READY"
	// StateLeased: A worker has claimed this task and holds an active lease token.
	StateLeased TaskState = "LEASED"
	// StateRunning: Worker is actively executing the task adapter.
	StateRunning TaskState = "RUNNING"
	// StateSucceeded: Task finished successfully; downstream dependent tasks can now unlock.
	StateSucceeded TaskState = "SUCCEEDED"
	// StateRetryWait: Task failed transiently; waiting for backoff timer before moving back to READY.
	StateRetryWait TaskState = "RETRY_WAIT"
	// StateFailed: Task failed and exhausted its retry budget (or fatal error).
	StateFailed TaskState = "FAILED"
	// StateDLQ: Dead Letter Queue for poison tasks that cannot be processed.
	StateDLQ TaskState = "DLQ"
)

// IsTerminal returns true if the task state is a final state (cannot transition further).
func (s TaskState) IsTerminal() bool {
	return s == StateSucceeded || s == StateFailed || s == StateDLQ
}

// Common errors for state transitions and graph validation
var (
	ErrInvalidStateTransition = errors.New("invalid task state transition")
	ErrCycleDetected          = errors.New("cycle detected in workflow DAG")
	ErrNodeNotFound           = errors.New("node not found in DAG")
	ErrDuplicateNode          = errors.New("duplicate node id in DAG")
)

// Node represents a static task definition within a workflow DAG.
type Node struct {
	ID         string            `json:"id"`
	Type       string            `json:"type"`       // e.g. "http", "python", "synthetic"
	Config     map[string]any    `json:"config"`     // task specific inputs
	MaxRetries int               `json:"max_retries"`
	TimeoutSec int               `json:"timeout_sec"`
}

// Edge represents a directed dependency: From -> To (From must finish before To can run).
type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// WorkflowDefinition is the static template of a workflow DAG.
type WorkflowDefinition struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Nodes       map[string]Node   `json:"nodes"`
	Edges       []Edge            `json:"edges"`
	CreatedAt   time.Time         `json:"created_at"`
}
