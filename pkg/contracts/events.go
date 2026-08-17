package contracts

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// EventType defines the type of domain event emitted across the cluster.
type EventType string

const (
	EventWorkflowStarted   EventType = "workflow.started"
	EventWorkflowSucceeded EventType = "workflow.succeeded"
	EventWorkflowFailed    EventType = "workflow.failed"

	EventTaskReady          EventType = "task.ready"
	EventTaskLeased         EventType = "task.leased"
	EventTaskStarted        EventType = "task.started"
	EventTaskSucceeded      EventType = "task.succeeded"
	EventTaskFailed         EventType = "task.failed"
	EventTaskRetryScheduled EventType = "task.retry_scheduled"
	EventTaskDLQ            EventType = "task.dlq"
	EventTaskReassigned     EventType = "task.reassigned"

	EventWorkerOffline      EventType = "worker.offline"
	EventWorkerHeartbeat    EventType = "worker.heartbeat"
)

// EventEnvelope is the uniform message wrapper used across Postgres Outbox,
// Valkey/Redis streams, and WebSocket fan-out to clients.
type EventEnvelope struct {
	EventID       string         `json:"event_id"`
	EventType     EventType      `json:"event_type"`
	WorkflowRunID string         `json:"workflow_run_id"`
	TaskRunID     *string        `json:"task_run_id,omitempty"`
	Attempt       int            `json:"attempt,omitempty"`
	OccurredAt    time.Time      `json:"occurred_at"`
	TraceID       string         `json:"trace_id,omitempty"`
	Payload       map[string]any `json:"payload,omitempty"`
}

// NewEvent creates a new EventEnvelope with a cryptographically unique UUID and UTC timestamp.
func NewEvent(
	eventType EventType,
	workflowRunID string,
	taskRunID *string,
	attempt int,
	traceID string,
	payload map[string]any,
) EventEnvelope {
	return EventEnvelope{
		EventID:       uuid.NewString(),
		EventType:     eventType,
		WorkflowRunID: workflowRunID,
		TaskRunID:     taskRunID,
		Attempt:       attempt,
		OccurredAt:    time.Now().UTC(),
		TraceID:       traceID,
		Payload:       payload,
	}
}

// ToJSON serializes the event envelope to a JSON byte slice.
func (e *EventEnvelope) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// FromJSON deserializes a JSON byte slice into an EventEnvelope.
func FromJSON(data []byte) (*EventEnvelope, error) {
	var env EventEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, err
	}
	return &env, nil
}
