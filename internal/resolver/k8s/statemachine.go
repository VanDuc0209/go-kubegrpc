package k8s

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/resolver"
)

// Clock is an interface for time operations, injectable for testing.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// realClock implements Clock using the real time package.
type realClock struct{}

func (realClock) Now() time.Time                        { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// StateMachine wraps a resolver.Resolver and tracks disconnection duration
// to implement the grace window behavior (R8.5).
//
// When the inner resolver transitions to Disconnected, the StateMachine records
// the time of disconnection. If the outage exceeds the configured graceWindow,
// it transitions to ResolverInGraceWindow and continues emitting the
// last-known endpoints so that RPCs are not interrupted by a transient API
// server outage. A WARN log is emitted at most once per graceWindow interval.
type StateMachine struct {
	inner       resolver.Resolver
	graceWindow time.Duration
	logger      *slog.Logger
	clock       Clock

	mu            sync.Mutex
	state         resolver.ResolverState
	disconnectedAt time.Time  // zero when not disconnected
	lastWarnAt    time.Time   // zero initially
	lastEndpoints []resolver.Endpoint // last-known endpoints when in grace window
}

// NewStateMachine creates a StateMachine that wraps inner and transitions to
// ResolverInGraceWindow when the cumulative disconnection exceeds graceWindow.
// If logger is nil, slog.Default() is used.
func NewStateMachine(inner resolver.Resolver, graceWindow time.Duration, logger *slog.Logger) *StateMachine {
	if logger == nil {
		logger = slog.Default()
	}
	return &StateMachine{
		inner:       inner,
		graceWindow: graceWindow,
		logger:      logger,
		clock:       realClock{},
	}
}

// Start implements resolver.Resolver. It delegates to the inner resolver but
// wraps the snapshot callback to apply grace-window logic.
//
// Snapshot handling:
//   - ResolverConnected: reset disconnection tracking, pass through as-is,
//     update lastEndpoints.
//   - ResolverDisconnected: start (or continue) the outage timer. Once the
//     cumulative outage exceeds graceWindow, switch to ResolverInGraceWindow
//     and emit the last-known endpoints. Before the threshold, pass through
//     the Disconnected snapshot but schedule a re-check after the remaining
//     grace duration so the transition fires promptly.
//   - ResolverInGraceWindow: pass through as-is (inner resolver should never
//     emit this state, but be defensive).
func (sm *StateMachine) Start(ctx context.Context, fn func(resolver.Snapshot)) error {
	wrapped := func(snap resolver.Snapshot) {
		sm.mu.Lock()
		defer sm.mu.Unlock()

		switch snap.State {
		case resolver.ResolverConnected:
			// Clear any outstanding outage tracking on reconnect.
			sm.disconnectedAt = time.Time{}
			sm.lastWarnAt = time.Time{}
			sm.state = resolver.ResolverConnected
			if len(snap.Endpoints) > 0 {
				// Keep a copy so we have something to serve during future outages.
				endpoints := make([]resolver.Endpoint, len(snap.Endpoints))
				copy(endpoints, snap.Endpoints)
				sm.lastEndpoints = endpoints
			}
			fn(snap)

		case resolver.ResolverDisconnected:
			now := sm.clock.Now()

			// Record the start of the outage the first time we see a
			// Disconnected snapshot in this outage cycle.
			if sm.disconnectedAt.IsZero() {
				sm.disconnectedAt = now
			}

			elapsed := now.Sub(sm.disconnectedAt)

			if elapsed >= sm.graceWindow {
				// Outage has exceeded the grace window — transition (or stay)
				// in InGraceWindow state.
				sm.state = resolver.ResolverInGraceWindow
				sm.maybeLogWarn(now)

				fn(resolver.Snapshot{
					Endpoints: sm.lastEndpoints,
					State:     resolver.ResolverInGraceWindow,
					Err:       snap.Err,
				})
			} else {
				// Still within the grace window — pass the Disconnected
				// snapshot through as-is and schedule a deferred check so
				// the transition to InGraceWindow fires as close to the
				// deadline as possible.
				sm.state = resolver.ResolverDisconnected
				remaining := sm.graceWindow - elapsed

				// Launch a goroutine that waits for the remaining grace
				// duration and, if still disconnected, emits an
				// InGraceWindow snapshot. This goroutine respects ctx
				// cancellation so it does not leak.
				go sm.scheduleGraceCheck(ctx, fn, remaining)

				fn(snap)
			}

		case resolver.ResolverInGraceWindow:
			// Defensive: pass through if the inner resolver somehow emits
			// this state (should not happen in practice).
			sm.state = resolver.ResolverInGraceWindow
			fn(snap)
		}
	}

	return sm.inner.Start(ctx, wrapped)
}

// Close implements resolver.Resolver. It delegates to the inner resolver.
func (sm *StateMachine) Close() error {
	return sm.inner.Close()
}

// scheduleGraceCheck waits for remaining duration and then, if the resolver is
// still in Disconnected state (not yet reconnected), emits an InGraceWindow
// snapshot with the last-known endpoints. The goroutine exits early when ctx is
// canceled so there are no leaks after Close.
func (sm *StateMachine) scheduleGraceCheck(ctx context.Context, fn func(resolver.Snapshot), remaining time.Duration) {
	select {
	case <-ctx.Done():
		return
	case <-sm.clock.After(remaining):
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Only act if we are still in the Disconnected state (i.e., the inner
	// resolver has not reconnected while we were waiting).
	if sm.state != resolver.ResolverDisconnected {
		return
	}

	now := sm.clock.Now()
	// Verify the outage is genuinely long enough — the clock may have drifted
	// or the disconnectedAt was reset and restarted.
	if sm.disconnectedAt.IsZero() || now.Sub(sm.disconnectedAt) < sm.graceWindow {
		return
	}

	sm.state = resolver.ResolverInGraceWindow
	sm.maybeLogWarn(now)

	fn(resolver.Snapshot{
		Endpoints: sm.lastEndpoints,
		State:     resolver.ResolverInGraceWindow,
	})
}

// maybeLogWarn emits a WARN log entry if at least graceWindow time has elapsed
// since the last warning (R8.5 — log at most once per graceWindow interval).
// Caller must hold sm.mu.
func (sm *StateMachine) maybeLogWarn(now time.Time) {
	if sm.lastWarnAt.IsZero() || now.Sub(sm.lastWarnAt) >= sm.graceWindow {
		sm.logger.Warn("k8s resolver: Kubernetes API unreachable beyond grace window; using last-known endpoints",
			slog.Duration("grace_window", sm.graceWindow),
			slog.Int("endpoint_count", len(sm.lastEndpoints)),
		)
		sm.lastWarnAt = now
	}
}
