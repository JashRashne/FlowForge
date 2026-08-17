package domain

import (
	"time"
)

// TaskRun represents an individual execution attempt of a specific node within a workflow run.
type TaskRun struct {
	ID             string         `json:"id"`
	WorkflowRunID  string         `json:"workflow_run_id"`
	NodeID         string         `json:"node_id"`
	State          TaskState      `json:"state"`
	Attempt        int            `json:"attempt"`
	LeaseOwner     *string        `json:"lease_owner,omitempty"`      // Worker ID currently holding the lease
	LeaseToken     *string        `json:"lease_token,omitempty"`      // Fencing token UUID
	LeaseExpiresAt *time.Time     `json:"lease_expires_at,omitempty"` // UTC lease expiry timestamp
	RetryAt        *time.Time     `json:"retry_at,omitempty"`         // Earliest time for retry backoff
	Input          map[string]any `json:"input,omitempty"`
	OutputRef      *string        `json:"output_ref,omitempty"` // Pointer/URI to output data
	Error          *string        `json:"error,omitempty"`      // Error message/JSON if failed
	CreatedAt      time.Time      `json:"created_at"`
	StartedAt      *time.Time     `json:"started_at,omitempty"`
	FinishedAt     *time.Time     `json:"finished_at,omitempty"`
	Version        int64          `json:"version"` // Optimistic concurrency version
}

// IsValidTransition evaluates if transitioning from 'from' state to 'to' state
// is legally allowed by the FlowForge state machine.
//
// Rules to enforce:
// 1. BLOCKED -> can only move to READY
// 2. READY -> can only move to LEASED
// 3. LEASED -> can move to RUNNING (worker begins) or READY (lease released/expired)
// 4. RUNNING -> can move to:
//   - SUCCEEDED (task completed successfully)
//   - RETRY_WAIT (task failed transiently, retries remaining)
//   - FAILED (task failed fatally or retries exhausted)
//   - READY (scheduler recovers expired lease)
//
// 5. RETRY_WAIT -> can only move to READY (once backoff interval expires)
// 6. FAILED -> can optionally move to DLQ (if classified as poison pill)
// 7. Terminal states SUCCEEDED and DLQ CANNOT transition to any other state.
func IsValidTransition(from, to TaskState) bool {
	switch from {
	case StateReady:
		return to == StateLeased
	case StateBlocked:
		return to == StateReady
	case StateLeased:
		return to == StateRunning || to == StateReady
	case StateRunning:
		return to == StateSucceeded || to == StateRetryWait || to == StateFailed || to == StateReady
	case StateRetryWait:
		return to == StateReady
	case StateFailed:
		return to == StateDLQ
	}
	return false

}
