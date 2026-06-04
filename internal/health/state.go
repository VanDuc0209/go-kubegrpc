package health

import (
	"context"
	"log/slog"
	"sync"
)

// String returns a human-readable name for the HealthState (R10.6).
func (h HealthState) String() string {
	switch h {
	case HealthServing:
		return "Serving"
	case HealthNotServing:
		return "NotServing"
	default:
		return "Unknown"
	}
}

// TransitionFunc is called whenever the health state transitions.
// The caller (balancer) uses this to update the pool and rebuild the picker.
type TransitionFunc func(newState HealthState)

// StateMachine tracks the health state for a single SubConn, processing
// HealthState events from a Checker and emitting transitions via a callback.
// Transitions: Unknown → Serving | NotServing, Serving ↔ NotServing.
// Each transition is published within 1s (R6.4, R6.5).
type StateMachine struct {
	mu       sync.Mutex
	current  HealthState
	onTrans  TransitionFunc
	logger   *slog.Logger
	endpoint string // for log messages (R10.6)
}

// NewStateMachine creates a health state machine for the given endpoint address.
// onTransition is called (from the goroutine running Drain) on every state change.
func NewStateMachine(endpoint string, onTransition TransitionFunc, logger *slog.Logger) *StateMachine {
	if logger == nil {
		logger = slog.Default()
	}
	return &StateMachine{
		current:  HealthUnknown,
		onTrans:  onTransition,
		logger:   logger,
		endpoint: endpoint,
	}
}

// Drain consumes events from ch until it is closed or ctx is canceled.
// Each event is compared to the current state; if different, it calls onTransition.
// Emits INFO log on every transition (R10.6).
func (sm *StateMachine) Drain(ctx context.Context, ch <-chan HealthState) {
	for {
		select {
		case <-ctx.Done():
			return
		case state, ok := <-ch:
			if !ok {
				return
			}
			sm.mu.Lock()
			if state != sm.current {
				sm.current = state
				sm.mu.Unlock()
				// Emit INFO log for the transition (R10.6).
				sm.logger.Info("health state transition",
					slog.String("endpoint", sm.endpoint),
					slog.String("state", state.String()),
				)
				sm.onTrans(state)
			} else {
				sm.mu.Unlock()
			}
		}
	}
}

// Current returns the current health state (thread-safe).
func (sm *StateMachine) Current() HealthState {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.current
}
