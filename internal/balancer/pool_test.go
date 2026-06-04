package balancer

import (
	"testing"
	"time"

	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/resolver"
)

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

// waitForShutdown polls sc.shutdown until it becomes true or the timeout expires.
// Returns true if shutdown was observed within the deadline.
func waitForShutdown(t *testing.T, sc *fakeSubConn, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		sc.mu.Lock()
		shutdown := sc.shutdown
		sc.mu.Unlock()
		if shutdown {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// ---------------------------------------------------------------------------
// Shared test fixtures
// ---------------------------------------------------------------------------

const (
	testNamespace = "test-ns"
	testService   = "test-svc"
)

func newTestPool() (*Pool, *fakeClientConn) {
	cc := newFakeClientConn()
	pool := NewPool(cc, nil, nil)
	return pool, cc
}

func addEndpoint(t *testing.T, pool *Pool, addr string, port int) (*SubConnEntry, *fakeSubConn) {
	t.Helper()
	ep := resolver.Endpoint{Address: addr, Port: port, Ready: true}
	entry, err := pool.Add(testNamespace, testService, ep)
	if err != nil {
		t.Fatalf("pool.Add(%s:%d) unexpected error: %v", addr, port, err)
	}
	sc, ok := entry.SubConn.(*fakeSubConn)
	if !ok {
		t.Fatalf("pool.Add returned a SubConn that is not *fakeSubConn")
	}
	return entry, sc
}

// ---------------------------------------------------------------------------
// TestPodRestartEviction – R4.6
//
// Adding the same key twice (simulating a Pod restart on the same address:port)
// must shut down the first SubConn and leave exactly one entry in the pool.
// ---------------------------------------------------------------------------

// TestPodRestartEviction verifies that when a new endpoint with the same key is
// added to the pool (Pod restart scenario), the previous SubConn is shut down
// and only one entry remains in the pool's Snapshot.
//
// Validates: Requirements 4.6
func TestPodRestartEviction(t *testing.T) {
	pool, _ := newTestPool()

	// First Add — represents the original Pod.
	_, sc1 := addEndpoint(t, pool, "10.0.0.1", 8080)

	// Second Add with the same key — simulates Pod restart reusing address:port.
	entry2, sc2 := addEndpoint(t, pool, "10.0.0.1", 8080)

	// The first SubConn must have been shut down immediately upon the second Add.
	sc1.mu.Lock()
	firstShutdown := sc1.shutdown
	sc1.mu.Unlock()
	if !firstShutdown {
		t.Error("expected first SubConn to be shut down after Pod restart eviction, but it was not")
	}

	// The second SubConn must still be alive.
	sc2.mu.Lock()
	secondShutdown := sc2.shutdown
	sc2.mu.Unlock()
	if secondShutdown {
		t.Error("expected second SubConn to be alive after creation, but it was shut down")
	}

	// The pool snapshot must contain exactly one entry for the key.
	snapshot := pool.Snapshot()
	if len(snapshot) != 1 {
		t.Errorf("expected exactly 1 entry in pool snapshot after restart eviction, got %d", len(snapshot))
	}
	if len(snapshot) == 1 && snapshot[0] != entry2 {
		t.Errorf("expected snapshot to contain the new entry, but it contains a different entry")
	}
}

// ---------------------------------------------------------------------------
// TestRemoveMarksDraining – R4.3
//
// After Remove, the entry is no longer present in Snapshot or HealthyEntries.
// Because InFlight is 0, the drainAndClose goroutine shuts the SubConn within
// a short time.
// ---------------------------------------------------------------------------

// TestRemoveMarksDraining verifies that Remove immediately excludes the entry
// from the pool's visible snapshot and healthy entries, and that the underlying
// SubConn is shut down promptly when there are zero in-flight RPCs.
//
// Validates: Requirements 4.3
func TestRemoveMarksDraining(t *testing.T) {
	pool, _ := newTestPool()

	_, sc := addEndpoint(t, pool, "10.0.0.2", 8080)

	key := PoolKey{
		Namespace: testNamespace,
		Service:   testService,
		Address:   "10.0.0.2",
		Port:      8080,
	}

	// Mark the entry as HealthServing so it would appear in HealthyEntries.
	pool.UpdateHealth(key, HealthServing)

	// Verify it is present before removal.
	if len(pool.Snapshot()) != 1 {
		t.Fatal("expected 1 entry before Remove")
	}
	if len(pool.HealthyEntries()) != 1 {
		t.Fatal("expected 1 healthy entry before Remove")
	}

	pool.Remove(key)

	// Immediately after Remove: entry must be absent from Snapshot and HealthyEntries.
	if len(pool.Snapshot()) != 0 {
		t.Error("expected 0 entries in Snapshot immediately after Remove")
	}
	if len(pool.HealthyEntries()) != 0 {
		t.Error("expected 0 healthy entries immediately after Remove")
	}

	// Since InFlight == 0, drainAndClose should shut the SubConn within ~50 ms.
	if !waitForShutdown(t, sc, 500*time.Millisecond) {
		t.Error("expected SubConn to be shut down within 500ms after Remove with zero in-flight RPCs")
	}
}

// ---------------------------------------------------------------------------
// TestDrainExcludesFromPicker – R4.3
//
// A draining entry must not appear in HealthyEntries even if it was Serving
// before removal.
// ---------------------------------------------------------------------------

// TestDrainExcludesFromPicker verifies that after Remove is called on a
// HealthServing endpoint, HealthyEntries returns an empty slice, ensuring
// draining connections are not routed new RPCs.
//
// Validates: Requirements 4.3
func TestDrainExcludesFromPicker(t *testing.T) {
	pool, _ := newTestPool()

	// Add two endpoints.
	_, _ = addEndpoint(t, pool, "10.0.0.3", 8080)
	_, _ = addEndpoint(t, pool, "10.0.0.4", 8080)

	key1 := PoolKey{Namespace: testNamespace, Service: testService, Address: "10.0.0.3", Port: 8080}
	key2 := PoolKey{Namespace: testNamespace, Service: testService, Address: "10.0.0.4", Port: 8080}

	// Mark both as HealthServing.
	pool.UpdateHealth(key1, HealthServing)
	pool.UpdateHealth(key2, HealthServing)

	// Both should appear in HealthyEntries.
	if len(pool.HealthyEntries()) != 2 {
		t.Fatalf("expected 2 healthy entries before any removal, got %d", len(pool.HealthyEntries()))
	}

	// Remove the first HealthServing endpoint.
	pool.Remove(key1)

	// Only key2 should remain in HealthyEntries; the draining key1 must be excluded.
	healthy := pool.HealthyEntries()
	if len(healthy) != 1 {
		t.Errorf("expected 1 healthy entry after removing one, got %d", len(healthy))
	}
	if len(healthy) == 1 && healthy[0].Key != key2 {
		t.Errorf("expected remaining healthy entry to be key2 (%v), got %v", key2, healthy[0].Key)
	}
}
