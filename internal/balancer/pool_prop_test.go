package balancer

import (
	"fmt"
	"sort"
	"sync"
	"testing"

	"pgregory.net/rapid"

	"google.golang.org/grpc/balancer"
	grpcresolver "google.golang.org/grpc/resolver"

	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/resolver"
)

// ---------------------------------------------------------------------------
// Fake gRPC plumbing (balancer.ClientConn + balancer.SubConn)
// ---------------------------------------------------------------------------

// fakeSubConn implements balancer.SubConn minimally for pool testing.
type fakeSubConn struct {
	mu       sync.Mutex
	id       int
	addr     string
	shutdown bool
}

func (f *fakeSubConn) UpdateAddresses(_ []grpcresolver.Address) {}
func (f *fakeSubConn) Connect()                                  {}
func (f *fakeSubConn) GetOrBuildProducer(balancer.ProducerBuilder) (balancer.Producer, func()) {
	return nil, func() {}
}
func (f *fakeSubConn) Shutdown() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shutdown = true
}

// fakeClientConn implements balancer.ClientConn for pool testing.
// It tracks every SubConn created so tests can inspect them.
type fakeClientConn struct {
	mu       sync.Mutex
	subconns map[string]*fakeSubConn // keyed by "addr:port" string
	nextID   int
}

func newFakeClientConn() *fakeClientConn {
	return &fakeClientConn{
		subconns: make(map[string]*fakeSubConn),
	}
}

func (f *fakeClientConn) NewSubConn(addrs []grpcresolver.Address, _ balancer.NewSubConnOptions) (balancer.SubConn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	addr := addrs[0].Addr
	sc := &fakeSubConn{id: f.nextID, addr: addr}
	f.nextID++
	f.subconns[addr] = sc
	return sc, nil
}

func (f *fakeClientConn) RemoveSubConn(_ balancer.SubConn)                     {}
func (f *fakeClientConn) UpdateAddresses(_ balancer.SubConn, _ []grpcresolver.Address) {}
func (f *fakeClientConn) UpdateState(_ balancer.State)                         {}
func (f *fakeClientConn) ResolveNow(_ grpcresolver.ResolveNowOptions)          {}
func (f *fakeClientConn) Target() string                                       { return "test-target" }

// ---------------------------------------------------------------------------
// Key universe for property-based testing
// ---------------------------------------------------------------------------

// smallAddresses × smallPorts gives 6 unique (address, port) pairs.
var (
	smallAddresses = []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
	smallPorts     = []int{8080, 9090}
)

// genEndpoint draws a random resolver.Endpoint from the small universe.
func genEndpoint(t *rapid.T, label string) resolver.Endpoint {
	addr := rapid.SampledFrom(smallAddresses).Draw(t, label+"_addr")
	port := rapid.SampledFrom(smallPorts).Draw(t, label+"_port")
	return resolver.Endpoint{Address: addr, Port: port, Ready: true}
}

// ---------------------------------------------------------------------------
// P6 – Subconn identity uniqueness (Validates: Requirements 4.5)
// ---------------------------------------------------------------------------

// TestPoolIdentityUniqueness verifies Property P6:
//
//	The pool holds at most one SubConn per (namespace, service, address, port)
//	tuple at all times, regardless of the sequence of Add / Remove operations.
//
// Strategy:
//   - Drive the pool with a random sequence of operations drawn from
//     {Add, Remove} on a small (address × port) universe.
//   - After each operation, call pool.Snapshot() and assert that no two
//     entries share the same PoolKey.
//
// Validates: Requirements 4.5
func TestPoolIdentityUniqueness(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		cc := newFakeClientConn()
		pool := NewPool(cc, nil, nil)

		const (
			namespace = "test-ns"
			service   = "test-svc"
		)

		// opCount drives how many random operations we apply.
		opCount := rapid.IntRange(1, 30).Draw(rt, "op_count")

		for i := range opCount {
			label := fmt.Sprintf("op%d", i)

			// Draw a random endpoint from the small universe.
			ep := genEndpoint(rt, label)

			// Randomly choose Add or Remove (50/50).
			doAdd := rapid.Bool().Draw(rt, label+"_is_add")

			if doAdd {
				// Add the endpoint. The pool must handle duplicate adds
				// (same key) by evicting the old SubConn first (R4.6).
				_, err := pool.Add(namespace, service, ep)
				if err != nil {
					rt.Fatalf("pool.Add(%v) returned unexpected error: %v", ep, err)
				}
			} else {
				key := PoolKey{
					Namespace: namespace,
					Service:   service,
					Address:   ep.Address,
					Port:      ep.Port,
				}
				pool.Remove(key)
			}

			// === Invariant: at most one entry per PoolKey ===
			snapshot := pool.Snapshot()
			seen := make(map[PoolKey]struct{}, len(snapshot))
			for _, entry := range snapshot {
				if _, exists := seen[entry.Key]; exists {
					rt.Fatalf("duplicate PoolKey %v found in pool snapshot after operation %d", entry.Key, i)
				}
				seen[entry.Key] = struct{}{}
			}
		}
	})
}

// ---------------------------------------------------------------------------
// P5 – MaxSubconnections cap honored (Validates: Requirements 12.1, 12.2)
// ---------------------------------------------------------------------------

// endpointKey returns the lexicographic sort key for a resolver.Endpoint.
func endpointKey(ep resolver.Endpoint) string {
	return fmt.Sprintf("%s:%d", ep.Address, ep.Port)
}

// TestCapEndpointsHonorsCap verifies Property P5:
//
//  1. len(result) <= M
//  2. len(result) == min(len(endpoints), M)  — no unexpected truncation or padding
//  3. The result is the lexicographically smallest M endpoints by "address:port"
//  4. result is a subset of the original endpoints
//
// Validates: Requirements 12.1, 12.2
func TestCapEndpointsHonorsCap(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Build a random slice of endpoints from addresses 10.0.0.1-3 and
		// ports 8080-8090 (small but distinct enough to stress ordering).
		addresses := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
		ports := []int{8080, 8081, 8082, 8083, 8084, 8085, 8086, 8087, 8088, 8089, 8090}

		rawEndpoints := rapid.SliceOfN(
			rapid.Custom(func(innerT *rapid.T) resolver.Endpoint {
				addr := rapid.SampledFrom(addresses).Draw(innerT, "ep_addr")
				port := rapid.SampledFrom(ports).Draw(innerT, "ep_port")
				return resolver.Endpoint{Address: addr, Port: port, Ready: true}
			}),
			0, 20,
		).Draw(rt, "endpoints")

		// De-duplicate endpoints by (address, port) because CapEndpoints
		// operates on a set-like logical model; duplicates in the input
		// would make the "subset" and "min count" assertions ambiguous.
		seen := make(map[string]struct{})
		var endpoints []resolver.Endpoint
		for _, ep := range rawEndpoints {
			k := endpointKey(ep)
			if _, dup := seen[k]; !dup {
				seen[k] = struct{}{}
				endpoints = append(endpoints, ep)
			}
		}

		// Random cap M ∈ [1, 10].
		M := rapid.IntRange(1, 10).Draw(rt, "max_subconns")

		result := CapEndpoints(endpoints, M)

		// ── Assertion 1: result length never exceeds M ──────────────────
		if len(result) > M {
			rt.Fatalf("len(result)=%d > M=%d", len(result), M)
		}

		// ── Assertion 2: result length == min(len(endpoints), M) ─────────
		expectedLen := len(endpoints)
		if expectedLen > M {
			expectedLen = M
		}
		if len(result) != expectedLen {
			rt.Fatalf("len(result)=%d != min(len(endpoints)=%d, M=%d)=%d",
				len(result), len(endpoints), M, expectedLen)
		}

		// ── Assertion 3: result is the lex-smallest M endpoints ──────────
		// Build the sorted-by-key view of all endpoints, take the first M.
		sorted := make([]resolver.Endpoint, len(endpoints))
		copy(sorted, endpoints)
		sort.Slice(sorted, func(i, j int) bool {
			return endpointKey(sorted[i]) < endpointKey(sorted[j])
		})
		expected := sorted
		if len(expected) > M {
			expected = sorted[:M]
		}

		for i, ep := range result {
			if endpointKey(ep) != endpointKey(expected[i]) {
				rt.Fatalf("result[%d]=%v but expected lex-smallest[%d]=%v\nresult=%v\nexpected=%v",
					i, ep, i, expected[i], result, expected)
			}
		}

		// ── Assertion 4: result is a subset of original endpoints ─────────
		origSet := make(map[string]struct{}, len(endpoints))
		for _, ep := range endpoints {
			origSet[endpointKey(ep)] = struct{}{}
		}
		for _, ep := range result {
			if _, ok := origSet[endpointKey(ep)]; !ok {
				rt.Fatalf("result contains endpoint %v not in original input", ep)
			}
		}
	})
}

// TestCapEndpointsNoCap verifies that CapEndpoints(endpoints, 0) returns
// all endpoints unchanged (unbounded mode, Requirements 12.1).
func TestCapEndpointsNoCap(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		addresses := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
		ports := []int{8080, 9090, 50051}

		endpoints := rapid.SliceOfN(
			rapid.Custom(func(innerT *rapid.T) resolver.Endpoint {
				addr := rapid.SampledFrom(addresses).Draw(innerT, "ep_addr")
				port := rapid.SampledFrom(ports).Draw(innerT, "ep_port")
				return resolver.Endpoint{Address: addr, Port: port, Ready: true}
			}),
			0, 15,
		).Draw(rt, "endpoints")

		result := CapEndpoints(endpoints, 0)

		// length must equal the original
		if len(result) != len(endpoints) {
			rt.Fatalf("CapEndpoints(endpoints, 0): len(result)=%d != len(endpoints)=%d",
				len(result), len(endpoints))
		}

		// Every element must match positionally (CapEndpoints returns a copy).
		for i := range endpoints {
			if result[i] != endpoints[i] {
				rt.Fatalf("CapEndpoints(endpoints, 0): result[%d]=%v != endpoints[%d]=%v",
					i, result[i], i, endpoints[i])
			}
		}
	})
}
