package contracts

import (
	"testing"
	"time"
)

func TestEventEnvelope_JSONRoundTrip(t *testing.T) {
	taskID := "task-123"
	payload := map[string]any{
		"task_type": "http",
		"priority":  float64(5),
	}

	event := NewEvent(
		EventTaskReady,
		"wf-run-456",
		&taskID,
		1,
		"trace-789",
		payload,
	)

	data, err := event.ToJSON()
	if err != nil {
		t.Fatalf("failed to marshal event to JSON: %v", err)
	}

	decoded, err := FromJSON(data)
	if err != nil {
		t.Fatalf("failed to unmarshal JSON into event: %v", err)
	}

	if decoded.EventID != event.EventID {
		t.Errorf("expected EventID %s, got %s", event.EventID, decoded.EventID)
	}
	if decoded.EventType != EventTaskReady {
		t.Errorf("expected EventType %s, got %s", EventTaskReady, decoded.EventType)
	}
	if decoded.TaskRunID == nil || *decoded.TaskRunID != taskID {
		t.Errorf("expected TaskRunID %s, got %v", taskID, decoded.TaskRunID)
	}
	if decoded.TraceID != "trace-789" {
		t.Errorf("expected TraceID 'trace-789', got %s", decoded.TraceID)
	}
	if decoded.Payload["task_type"] != "http" {
		t.Errorf("expected payload task_type 'http', got %v", decoded.Payload["task_type"])
	}

	// Verify timestamp is reasonably recent (within 5 seconds)
	if time.Since(decoded.OccurredAt) > 5*time.Second {
		t.Errorf("unexpected event age: %v", time.Since(decoded.OccurredAt))
	}
}
