package balancer

import (
	"sync"
	"testing"
	"time"

	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/resolver"
)

// ---------------------------------------------------------------------------
// Task 15.1 – Race-detector integration tests
// ---------------------------------------------------------------------------

// TestPoolConcurrentAddRemove spawns 10 goroutines that each perform 5 random
// Add operations and 5 random Remove operations on a shared Pool, all
// concurrently. Under `go test -race` any unsynchronised access will trigger
// the race detector and fail the test.
//
// Validates: Requirements 8.1, 8.2, 8.3, 8.4
func TestPoolConcurrentAddRemove(t *testing.T) {
	cc := newFakeClientConn()
	pool := NewPool(cc, nil, nil)

	const (
		goroutines = 10
		opsEach    = 5
		namespace  = "test-ns"
		service    = "test-svc"
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		g := g // capture loop variable
		go func() {
			defer wg.Done()

			// 5 Add operations cycling through the small key universe.
			for i := 0; i < opsEach; i++ {
				addr := smallAddresses[(g+i)%len(smallAddresses)]
				port := smallPorts[i%len(smallPorts)]
				ep := resolver.Endpoint{Address: addr, Port: port, Ready: true}
				// Ignore errors – concurrent adds on the same key are valid
				// and handled by the pool via eviction (R4.6).
				_, _ = pool.Add(namespace, service, ep)
			}

			// 5 Remove operations on the same universe.
			for i := 0; i < opsEach; i++ {
				addr := smallAddresses[(g+i+1)%len(smallAddresses)]
				port := smallPorts[(i+1)%len(smallPorts)]
				key := PoolKey{
					Namespace: namespace,
					Service:   service,
					Address:   addr,
					Port:      port,
				}
				pool.Remove(key)
			}
		}()
	}

	wg.Wait()

	// Both methods must be callable after concurrent operations without panic.
	_ = pool.Snapshot()
	_ = pool.HealthyEntries()
}

// TestBuilderSetActiveConfigConcurrent spawns 5 writer goroutines calling
// SetActiveConfig and 5 reader goroutines calling getActiveConfig, all
// concurrently. Under `go test -race` any data race on the package-level
// activeConfig variable will cause a failure.
//
// Validates: Requirements 8.1, 8.2
func TestBuilderSetActiveConfigConcurrent(t *testing.T) {
	const goroutines = 5

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Writers.
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			SetActiveConfig(&BuildConfig{
				Namespace:   "ns",
				Service:     "svc",
				Policy:      "round_robin",
				MaxSubconns: i,
			})
		}()
	}

	// Readers – call getActiveConfig which is in the same package.
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			cfg := getActiveConfig()
			// Use cfg to prevent the compiler from optimising the call away.
			_ = cfg.Namespace
		}()
	}

	wg.Wait()

	// Restore a clean state so other tests are not affected.
	SetActiveConfig(nil)
}

// ---------------------------------------------------------------------------
// Task 15.2 – Recovery integration tests (scale-up, scale-down, outage)
// ---------------------------------------------------------------------------

// waitForSnapshotLen polls pool.Snapshot() until its length equals want or
// the timeout expires. Returns true when the condition is met.
func waitForSnapshotLen(pool *Pool, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(pool.Snapshot()) == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// makeKey builds a PoolKey for the shared test namespace / service.
func makeKey(addr string, port int) PoolKey {
	return PoolKey{
		Namespace: testNamespace,
		Service:   testService,
		Address:   addr,
		Port:      port,
	}
}

// TestPoolScaleUpDown exercises the scale-up → scale-down → re-add lifecycle
// of a Pool, validating that Snapshot reflects each transition correctly.
//
// Validates: Requirements 8.1, 8.2, 8.3, 8.4
func TestPoolScaleUpDown(t *testing.T) {
	cc := newFakeClientConn()
	pool := NewPool(cc, nil, nil)

	ep := func(addr string, port int) resolver.Endpoint {
		return resolver.Endpoint{Address: addr, Port: port, Ready: true}
	}

	// ── Step 1: start with 0 endpoints ──────────────────────────────────────
	if len(pool.Snapshot()) != 0 {
		t.Fatalf("expected empty pool at start, got %d entries", len(pool.Snapshot()))
	}

	// ── Step 2: scale up – add 3 endpoints ──────────────────────────────────
	for _, addr := range []string{"10.1.0.1", "10.1.0.2", "10.1.0.3"} {
		if _, err := pool.Add(testNamespace, testService, ep(addr, 8080)); err != nil {
			t.Fatalf("pool.Add(%s:8080) error: %v", addr, err)
		}
	}

	if snap := pool.Snapshot(); len(snap) != 3 {
		t.Fatalf("expected 3 entries after scale-up, got %d", len(snap))
	}

	// ── Step 3: scale down – remove 1 endpoint ──────────────────────────────
	removedKey := makeKey("10.1.0.1", 8080)
	pool.Remove(removedKey)

	// Snapshot must reflect the removal within 50 ms (the remove is synchronous
	// in the pool; the async goroutine only handles SubConn.Shutdown).
	if !waitForSnapshotLen(pool, 2, 50*time.Millisecond) {
		t.Fatalf("expected 2 entries within 50ms after removal, got %d", len(pool.Snapshot()))
	}

	// The removed entry must not appear in Snapshot.
	for _, entry := range pool.Snapshot() {
		if entry.Key == removedKey {
			t.Errorf("removed key %v still present in Snapshot", removedKey)
		}
	}

	// ── Step 4: re-add the removed endpoint ─────────────────────────────────
	if _, err := pool.Add(testNamespace, testService, ep("10.1.0.1", 8080)); err != nil {
		t.Fatalf("pool.Add(10.1.0.1:8080) after re-add: %v", err)
	}

	if snap := pool.Snapshot(); len(snap) != 3 {
		t.Fatalf("expected 3 entries after re-add, got %d", len(snap))
	}
}

// TestPoolScaleDownSetsCorrectlyRemoved adds endpoints A, B, C and then
// removes A and B, verifying that only C remains in Snapshot.
//
// Validates: Requirements 8.1, 8.2, 8.3, 8.4
func TestPoolScaleDownSetsCorrectlyRemoved(t *testing.T) {
	cc := newFakeClientConn()
	pool := NewPool(cc, nil, nil)

	type endpoint struct {
		addr string
		port int
	}

	A := endpoint{"10.2.0.1", 8080}
	B := endpoint{"10.2.0.2", 8080}
	C := endpoint{"10.2.0.3", 8080}

	// Add A, B, C.
	for _, e := range []endpoint{A, B, C} {
		ep := resolver.Endpoint{Address: e.addr, Port: e.port, Ready: true}
		if _, err := pool.Add(testNamespace, testService, ep); err != nil {
			t.Fatalf("pool.Add(%s:%d) error: %v", e.addr, e.port, err)
		}
	}

	if len(pool.Snapshot()) != 3 {
		t.Fatalf("expected 3 entries after adding A, B, C")
	}

	// Remove A and B.
	pool.Remove(makeKey(A.addr, A.port))
	pool.Remove(makeKey(B.addr, B.port))

	// Only C must remain.
	snap := pool.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 entry after removing A and B, got %d", len(snap))
	}

	keyC := makeKey(C.addr, C.port)
	if snap[0].Key != keyC {
		t.Errorf("expected remaining entry to be C (%v), got %v", keyC, snap[0].Key)
	}

	// A and B must not appear.
	for _, entry := range pool.Snapshot() {
		if entry.Key == makeKey(A.addr, A.port) {
			t.Errorf("removed entry A (%v) still present in Snapshot", entry.Key)
		}
		if entry.Key == makeKey(B.addr, B.port) {
			t.Errorf("removed entry B (%v) still present in Snapshot", entry.Key)
		}
	}
}
