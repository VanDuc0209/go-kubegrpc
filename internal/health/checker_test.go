package health

import (
	"context"
	"testing"
	"time"
)

// TestEnableHealthCheckFalse verifies R6.7: when EnableCheck is false, Watch immediately
// emits HealthServing and closes the channel without issuing any probes.
func TestEnableHealthCheckFalse(t *testing.T) {
	t.Parallel()

	checker := NewChecker(CheckerConfig{
		EnableCheck: false,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// conn is nil — must not be touched when EnableCheck is false.
	ch, err := checker.Watch(ctx, nil)
	if err != nil {
		t.Fatalf("Watch returned unexpected error: %v", err)
	}

	// First event must be HealthServing.
	select {
	case state, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before emitting any state")
		}
		if state != HealthServing {
			t.Fatalf("expected HealthServing, got %v", state)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for first HealthServing event")
	}

	// Channel must be closed after the single event.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed, but received a second value")
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for channel to be closed")
	}
}

// TestStateMachineDrain verifies R6.3, R6.4, R6.5: the StateMachine correctly tracks
// health transitions and calls onTransition for every state change.
func TestStateMachineDrain(t *testing.T) {
	t.Parallel()

	var transitions []HealthState
	sm := NewStateMachine("host:1234", func(s HealthState) {
		transitions = append(transitions, s)
	}, nil)

	ch := make(chan HealthState, 8)

	// Sequence: Unknown→Serving, Serving→NotServing, NotServing→Serving.
	// StateMachine starts at HealthUnknown, so the first event (HealthUnknown) is a
	// duplicate and must be suppressed; HealthServing is a real transition.
	ch <- HealthUnknown    // duplicate of initial state — no transition expected
	ch <- HealthServing    // Unknown → Serving
	ch <- HealthNotServing // Serving → NotServing
	ch <- HealthServing    // NotServing → Serving
	close(ch)

	ctx := context.Background()
	sm.Drain(ctx, ch)

	// Three genuine transitions expected.
	expected := []HealthState{HealthServing, HealthNotServing, HealthServing}
	if len(transitions) != len(expected) {
		t.Fatalf("expected %d transitions, got %d: %v", len(expected), len(transitions), transitions)
	}
	for i, want := range expected {
		if transitions[i] != want {
			t.Errorf("transition[%d]: expected %v, got %v", i, want, transitions[i])
		}
	}

	// Current state must reflect the last event.
	if got := sm.Current(); got != HealthServing {
		t.Errorf("Current(): expected HealthServing, got %v", got)
	}
}

// TestStateMachineNoDuplicateTransitions verifies that sending the same state twice in a
// row triggers onTransition only once.
func TestStateMachineNoDuplicateTransitions(t *testing.T) {
	t.Parallel()

	callCount := 0
	sm := NewStateMachine("host:5678", func(s HealthState) {
		callCount++
	}, nil)

	ch := make(chan HealthState, 4)
	ch <- HealthServing // first transition: Unknown → Serving
	ch <- HealthServing // duplicate — must be ignored
	close(ch)

	sm.Drain(context.Background(), ch)

	if callCount != 1 {
		t.Errorf("expected onTransition to be called once, got %d calls", callCount)
	}

	if got := sm.Current(); got != HealthServing {
		t.Errorf("Current(): expected HealthServing, got %v", got)
	}
}

// TestStateMachineChannelClosed verifies R6.4, R6.5: Drain exits cleanly when the source
// channel is closed and transitions are recorded in order.
func TestStateMachineChannelClosed(t *testing.T) {
	t.Parallel()

	var transitions []HealthState
	sm := NewStateMachine("host:9999", func(s HealthState) {
		transitions = append(transitions, s)
	}, nil)

	ch := make(chan HealthState, 4)
	ch <- HealthNotServing
	ch <- HealthServing
	close(ch)

	sm.Drain(context.Background(), ch)

	expected := []HealthState{HealthNotServing, HealthServing}
	if len(transitions) != len(expected) {
		t.Fatalf("expected %d transitions, got %d: %v", len(expected), len(transitions), transitions)
	}
	for i, want := range expected {
		if transitions[i] != want {
			t.Errorf("transition[%d]: expected %v, got %v", i, want, transitions[i])
		}
	}

	if got := sm.Current(); got != HealthServing {
		t.Errorf("Current(): expected HealthServing, got %v", got)
	}
}

// TestStateMachineContextCancel verifies that Drain returns promptly when ctx is canceled,
// even when the source channel is never closed.
func TestStateMachineContextCancel(t *testing.T) {
	t.Parallel()

	sm := NewStateMachine("host:0000", func(s HealthState) {}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan HealthState) // unbuffered; never written to

	done := make(chan struct{})
	go func() {
		defer close(done)
		sm.Drain(ctx, ch)
	}()

	cancel() // trigger context cancellation

	select {
	case <-done:
		// Drain exited as expected.
	case <-time.After(time.Second):
		t.Fatal("Drain did not return after context cancellation within 1s")
	}
}
