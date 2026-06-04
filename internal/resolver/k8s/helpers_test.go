package k8s

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"

	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/resolver"
)

// ptr32 returns a pointer to the given int32 value.
func ptr32(n int32) *int32 { return &n }

// ptrStr returns a pointer to the given string value.
func ptrStr(s string) *string { return &s }

// ---------------------------------------------------------------------------
// matchPort tests (Requirements: 3.7)
// ---------------------------------------------------------------------------

// TestMatchPort_Numeric verifies that matchPort correctly identifies a port by
// its numeric value and returns (0, false) when the number is not present.
// Requirements: 3.7
func TestMatchPort_Numeric(t *testing.T) {
	t.Parallel()

	ports := []discoveryv1.EndpointPort{
		{
			Name: ptrStr("http"),
			Port: ptr32(8080),
		},
	}

	t.Run("match found", func(t *testing.T) {
		t.Parallel()
		got, ok := matchPort(ports, Port{Number: 8080})
		if !ok {
			t.Fatal("expected ok=true, got false")
		}
		if got != 8080 {
			t.Errorf("expected port 8080, got %d", got)
		}
	})

	t.Run("match not found", func(t *testing.T) {
		t.Parallel()
		got, ok := matchPort(ports, Port{Number: 9090})
		if ok {
			t.Fatal("expected ok=false, got true")
		}
		if got != 0 {
			t.Errorf("expected port 0, got %d", got)
		}
	})
}

// TestMatchPort_Named verifies that matchPort correctly identifies a port by
// its name and returns (0, false) when the name is not present.
// Requirements: 3.7
func TestMatchPort_Named(t *testing.T) {
	t.Parallel()

	ports := []discoveryv1.EndpointPort{
		{
			Name: ptrStr("grpc"),
			Port: ptr32(50051),
		},
	}

	t.Run("match found", func(t *testing.T) {
		t.Parallel()
		got, ok := matchPort(ports, Port{Name: "grpc"})
		if !ok {
			t.Fatal("expected ok=true, got false")
		}
		if got != 50051 {
			t.Errorf("expected port 50051, got %d", got)
		}
	})

	t.Run("match not found", func(t *testing.T) {
		t.Parallel()
		got, ok := matchPort(ports, Port{Name: "http"})
		if ok {
			t.Fatal("expected ok=false, got true")
		}
		if got != 0 {
			t.Errorf("expected port 0, got %d", got)
		}
	})
}

// ---------------------------------------------------------------------------
// reconnectBackoff tests (Requirements: 3.5)
// ---------------------------------------------------------------------------

// TestReconnectBackoffSequence verifies that the backoff table is monotonically
// non-decreasing, starts at 1s, ends at 30s, and that clamped indexing
// (min(i, len-1)) never returns a value greater than 30s.
// Requirements: 3.5
func TestReconnectBackoffSequence(t *testing.T) {
	t.Parallel()

	if len(reconnectBackoff) == 0 {
		t.Fatal("reconnectBackoff must not be empty")
	}

	// First element must be 1s.
	if reconnectBackoff[0] != 1*time.Second {
		t.Errorf("expected reconnectBackoff[0]=1s, got %v", reconnectBackoff[0])
	}

	// Sequence must be monotonically non-decreasing.
	for i := 1; i < len(reconnectBackoff); i++ {
		if reconnectBackoff[i] < reconnectBackoff[i-1] {
			t.Errorf("backoff is not monotone at index %d: %v < %v",
				i, reconnectBackoff[i], reconnectBackoff[i-1])
		}
	}

	// Last element must be capped at 30s.
	last := reconnectBackoff[len(reconnectBackoff)-1]
	if last != 30*time.Second {
		t.Errorf("expected last reconnectBackoff element=30s, got %v", last)
	}

	// Clamped index access must never exceed 30s.
	// Simulate what watcher.go does: min(backoffIdx, len(reconnectBackoff)-1).
	maxIdx := len(reconnectBackoff) - 1
	for i := 0; i <= maxIdx+10; i++ {
		idx := min(i, maxIdx)
		if reconnectBackoff[idx] > 30*time.Second {
			t.Errorf("clamped index %d (backoff[%d]=%v) exceeds 30s",
				i, idx, reconnectBackoff[idx])
		}
	}
}

// ---------------------------------------------------------------------------
// StateMachine transition tests (Requirements: 8.5)
// ---------------------------------------------------------------------------

// fakeClock implements Clock with a controllable "now" value.
// After returns the provided channel rather than a real timer, giving tests
// full control over when the grace check fires.
type fakeClock struct {
	mu      sync.Mutex
	nowTime time.Time
	afterCh chan time.Time
}

func newFakeClock(t time.Time) *fakeClock {
	return &fakeClock{
		nowTime: t,
		afterCh: make(chan time.Time), // never fires unless test sends to it
	}
}

func (fc *fakeClock) Now() time.Time {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.nowTime
}

func (fc *fakeClock) After(_ time.Duration) <-chan time.Time {
	return fc.afterCh
}

func (fc *fakeClock) advance(d time.Duration) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.nowTime = fc.nowTime.Add(d)
}

// fakeResolver stores the snapshot callback so tests can inject snapshots
// directly without needing a real Kubernetes API server.
type fakeResolver struct {
	mu sync.Mutex
	fn func(resolver.Snapshot)
}

func (f *fakeResolver) Start(_ context.Context, fn func(resolver.Snapshot)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fn = fn
	return nil
}

func (f *fakeResolver) Close() error { return nil }

func (f *fakeResolver) send(snap resolver.Snapshot) {
	f.mu.Lock()
	fn := f.fn
	f.mu.Unlock()
	if fn != nil {
		fn(snap)
	}
}

// warnCounter is a slog.Handler that counts WARN-level records.
type warnCounter struct {
	mu    sync.Mutex
	count int
}

func (w *warnCounter) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelWarn
}

func (w *warnCounter) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		w.mu.Lock()
		w.count++
		w.mu.Unlock()
	}
	return nil
}

func (w *warnCounter) WithAttrs(_ []slog.Attr) slog.Handler  { return w }
func (w *warnCounter) WithGroup(_ string) slog.Handler       { return w }

func (w *warnCounter) warnCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.count
}

// TestStateMachineTransitions verifies that StateMachine correctly transitions
// between Connected, Disconnected, and InGraceWindow states based on elapsed
// time, using a fake clock to control timing precisely.
// Requirements: 8.5
func TestStateMachineTransitions(t *testing.T) {
	t.Parallel()

	const graceWindow = 50 * time.Millisecond

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	fc := newFakeClock(base)

	inner := &fakeResolver{}
	wc := &warnCounter{}
	logger := slog.New(wc)

	sm := NewStateMachine(inner, graceWindow, logger)
	sm.clock = fc

	// Collect snapshots emitted to the outer callback.
	var (
		snapMu    sync.Mutex
		snapshots []resolver.Snapshot
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := sm.Start(ctx, func(s resolver.Snapshot) {
		snapMu.Lock()
		snapshots = append(snapshots, s)
		snapMu.Unlock()
	})
	if err != nil {
		t.Fatalf("StateMachine.Start returned unexpected error: %v", err)
	}

	lastSnapshots := func() []resolver.Snapshot {
		snapMu.Lock()
		defer snapMu.Unlock()
		out := make([]resolver.Snapshot, len(snapshots))
		copy(out, snapshots)
		return out
	}

	// -----------------------------------------------------------------------
	// Step 1: inject a Connected snapshot with known endpoints.
	// This sets lastEndpoints so the grace window path has data to serve.
	// -----------------------------------------------------------------------
	endpoints := []resolver.Endpoint{
		{Address: "10.0.0.1", Port: 9000, Ready: true},
		{Address: "10.0.0.2", Port: 9000, Ready: true},
	}
	inner.send(resolver.Snapshot{
		State:     resolver.ResolverConnected,
		Endpoints: endpoints,
	})

	// The Connected snapshot must be forwarded as-is.
	snaps := lastSnapshots()
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot after Connected, got %d", len(snaps))
	}
	if snaps[0].State != resolver.ResolverConnected {
		t.Errorf("expected State=Connected, got %v", snaps[0].State)
	}

	// -----------------------------------------------------------------------
	// Step 2: inject a Disconnected snapshot within the grace window.
	// The elapsed time is 0, so we are still within graceWindow.
	// The state machine must pass through as ResolverDisconnected.
	// -----------------------------------------------------------------------
	inner.send(resolver.Snapshot{State: resolver.ResolverDisconnected})

	snaps = lastSnapshots()
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshots after first Disconnected, got %d", len(snaps))
	}
	if snaps[1].State != resolver.ResolverDisconnected {
		t.Errorf("expected State=Disconnected, got %v", snaps[1].State)
	}
	if wc.warnCount() != 0 {
		t.Errorf("expected 0 WARN logs within grace window, got %d", wc.warnCount())
	}

	// -----------------------------------------------------------------------
	// Step 3: advance time past the grace window and inject another
	// Disconnected snapshot. The state machine must now transition to
	// InGraceWindow and emit the last-known endpoints.
	// -----------------------------------------------------------------------
	fc.advance(graceWindow + 1*time.Millisecond)
	inner.send(resolver.Snapshot{State: resolver.ResolverDisconnected})

	snaps = lastSnapshots()
	if len(snaps) != 3 {
		t.Fatalf("expected 3 snapshots after InGraceWindow transition, got %d", len(snaps))
	}
	if snaps[2].State != resolver.ResolverInGraceWindow {
		t.Errorf("expected State=InGraceWindow, got %v", snaps[2].State)
	}
	if len(snaps[2].Endpoints) != len(endpoints) {
		t.Errorf("expected %d last-known endpoints in InGraceWindow snapshot, got %d",
			len(endpoints), len(snaps[2].Endpoints))
	}
	if wc.warnCount() != 1 {
		t.Errorf("expected exactly 1 WARN log after grace window exceeded, got %d", wc.warnCount())
	}

	// -----------------------------------------------------------------------
	// Step 4: send a Connected snapshot — state machine must reset.
	// -----------------------------------------------------------------------
	inner.send(resolver.Snapshot{
		State:     resolver.ResolverConnected,
		Endpoints: endpoints,
	})

	snaps = lastSnapshots()
	if len(snaps) != 4 {
		t.Fatalf("expected 4 snapshots after reconnect, got %d", len(snaps))
	}
	if snaps[3].State != resolver.ResolverConnected {
		t.Errorf("expected State=Connected after reconnect, got %v", snaps[3].State)
	}

	// -----------------------------------------------------------------------
	// Step 5: verify WARN is not logged a second time within the same
	// graceWindow interval after the reconnect reset tracking.
	// -----------------------------------------------------------------------
	warnsBefore := wc.warnCount()
	inner.send(resolver.Snapshot{State: resolver.ResolverDisconnected})
	// elapsed since reconnect is 0; still within grace window.
	snaps = lastSnapshots()
	last := snaps[len(snaps)-1]
	if last.State != resolver.ResolverDisconnected {
		t.Errorf("expected Disconnected snapshot after reconnect, got %v", last.State)
	}
	if wc.warnCount() != warnsBefore {
		t.Errorf("unexpected WARN log within grace window after reconnect: count=%d", wc.warnCount())
	}
}
