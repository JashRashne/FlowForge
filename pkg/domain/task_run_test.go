package domain

import (
	"testing"
)

func TestIsValidTransition(t *testing.T) {
	// Table-driven test cases
	tests := []struct {
		name     string
		from     TaskState
		to       TaskState
		expected bool
	}{
		// Valid transitions
		{"BLOCKED to READY", StateBlocked, StateReady, true},
		{"READY to LEASED", StateReady, StateLeased, true},
		{"LEASED to RUNNING", StateLeased, StateRunning, true},
		{"LEASED back to READY (abandoned lease)", StateLeased, StateReady, true},
		{"RUNNING to SUCCEEDED", StateRunning, StateSucceeded, true},
		{"RUNNING to RETRY_WAIT", StateRunning, StateRetryWait, true},
		{"RUNNING to FAILED", StateRunning, StateFailed, true},
		{"RUNNING back to READY (scheduler lease recovery)", StateRunning, StateReady, true},
		{"RETRY_WAIT to READY", StateRetryWait, StateReady, true},
		{"FAILED to DLQ", StateFailed, StateDLQ, true},

		// Invalid transitions
		{"BLOCKED directly to RUNNING", StateBlocked, StateRunning, false},
		{"BLOCKED directly to SUCCEEDED", StateBlocked, StateSucceeded, false},
		{"READY directly to SUCCEEDED", StateReady, StateSucceeded, false},
		{"SUCCEEDED to RUNNING (terminal state violation)", StateSucceeded, StateRunning, false},
		{"SUCCEEDED to READY", StateSucceeded, StateReady, false},
		{"DLQ to READY (terminal state violation)", StateDLQ, StateReady, false},
		{"RETRY_WAIT directly to RUNNING", StateRetryWait, StateRunning, false},
		{"Same state transition (READY to READY)", StateReady, StateReady, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := IsValidTransition(tc.from, tc.to)
			if actual != tc.expected {
				t.Errorf("transition from %s to %s: expected %v, got %v", tc.from, tc.to, tc.expected, actual)
			}
		})
	}
}
