package balancer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	grpcresolver "google.golang.org/grpc/resolver"

	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/resolver"
)

// PoolKey uniquely identifies a subconnection within the pool.
// The (namespace, service, address, port) tuple enforces at most one SubConn
// per logical endpoint (R4.5).
type PoolKey struct {
	Namespace string
	Service   string
	Address   string
	Port      int
}

// String returns a human-readable representation of the key.
func (k PoolKey) String() string {
	return fmt.Sprintf("%s/%s/%s:%d", k.Namespace, k.Service, k.Address, k.Port)
}

// addrPort returns the "address:port" string used for gRPC address construction
// and for deterministic lexicographic ordering.
func (k PoolKey) addrPort() string {
	return fmt.Sprintf("%s:%d", k.Address, k.Port)
}

// HealthState represents the health-check state of a subconnection as reported
// by the Health Checker (R6).
type HealthState int

const (
	// HealthUnknown is the initial state before a health probe has completed.
	HealthUnknown HealthState = iota
	// HealthServing indicates the subconnection is healthy and eligible for picks.
	HealthServing
	// HealthNotServing indicates the subconnection failed a health probe.
	HealthNotServing
)

// String returns a human-readable name for the HealthState.
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

// SubConnEntry holds the full state for one subconnection managed by the Pool.
//
// InFlight uses atomic.Int64 so pickers can increment/decrement it on the hot
// RPC path without holding the Pool mutex.
type SubConnEntry struct {
	Key          PoolKey
	SubConn      balancer.SubConn
	InFlight     atomic.Int64 // incremented by picker, decremented via Done callback
	Draining     bool         // true once marked for removal (R4.3)
	Health       HealthState  // current health state as reported by the Health Checker
	ConnState    connectivity.State
	CreatedAt    time.Time
	HealthCancel func() // stops the active health checker (Task 3)
}

// Pool manages the set of SubConnections for a single logical target,
// enforcing uniqueness by PoolKey and managing the full SubConn lifecycle.
// All exported methods are safe for concurrent use.
type Pool struct {
	mu         sync.Mutex
	entries    map[PoolKey]*SubConnEntry
	cc         balancer.ClientConn // gRPC client conn used to create/destroy SubConns
	logger     *slog.Logger
	reverifyFn func(PoolKey) // called when a SubConn enters permanent failure (R4.4)
}

// NewPool creates a new, empty Pool.
//
//   - cc is the gRPC balancer.ClientConn used to create SubConns.
//   - logger is used for structured INFO logs on SubConn lifecycle events (R4.7).
//   - reverifyFn is called when a SubConn enters connectivity.TransientFailure so
//     the Resolver can verify the backing Endpoint still exists (R4.4).
func NewPool(cc balancer.ClientConn, logger *slog.Logger, reverifyFn func(PoolKey)) *Pool {
	if logger == nil {
		logger = slog.Default()
	}
	return &Pool{
		entries:    make(map[PoolKey]*SubConnEntry),
		cc:         cc,
		logger:     logger,
		reverifyFn: reverifyFn,
	}
}

// Add creates a new SubConn for the given endpoint.
//
// Key = PoolKey{Namespace, Service, ep.Address, ep.Port}.
//
// If a SubConn for that key already exists (e.g., Pod restarted and reused the
// same address:port), the existing SubConn is closed first before creating the
// new one (R4.6). Exactly one SubConn per key is maintained at all times (R4.5).
//
// Returns the newly created SubConnEntry, or an error if SubConn creation fails.
func (p *Pool) Add(namespace, service string, ep resolver.Endpoint) (*SubConnEntry, error) {
	key := PoolKey{
		Namespace: namespace,
		Service:   service,
		Address:   ep.Address,
		Port:      ep.Port,
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// R4.6: if the key already exists, close the old SubConn before creating a
	// new one. This handles Pod restarts that reuse the same address:port.
	if existing, ok := p.entries[key]; ok {
		p.logger.Info("subconn evicted before recreation",
			slog.String("endpoint", key.addrPort()),
			slog.String("namespace", key.Namespace),
			slog.String("service", key.Service),
			slog.String("reason", "pod_restart_eviction"),
		)
		if existing.HealthCancel != nil {
			existing.HealthCancel()
		}
		existing.SubConn.Shutdown()
		delete(p.entries, key)
	}

	// R4.1: create exactly one SubConn per endpoint address:port.
	addr := grpcresolver.Address{Addr: key.addrPort()}
	sc, err := p.cc.NewSubConn([]grpcresolver.Address{addr}, balancer.NewSubConnOptions{})
	if err != nil {
		return nil, fmt.Errorf("pool: NewSubConn for %s: %w", key, err)
	}

	entry := &SubConnEntry{
		Key:       key,
		SubConn:   sc,
		Health:    HealthUnknown,
		ConnState: connectivity.Idle,
		CreatedAt: time.Now(),
	}
	p.entries[key] = entry

	// R4.7: emit INFO log on SubConn creation.
	p.logger.Info("subconn created",
		slog.String("endpoint", key.addrPort()),
		slog.String("namespace", key.Namespace),
		slog.String("service", key.Service),
		slog.String("reason", "created"),
	)

	return entry, nil
}

// Remove marks the SubConn for the given key as draining and initiates an
// asynchronous close sequence.
//
// While draining, the SubConn is excluded from new picks (R4.3). A background
// goroutine waits for the in-flight RPC count to reach zero or for a 5-second
// deadline to expire, whichever comes first, and then shuts down the SubConn
// (R4.2).
//
// If the key does not exist in the pool, Remove is a no-op.
func (p *Pool) Remove(key PoolKey) {
	p.mu.Lock()
	entry, ok := p.entries[key]
	if !ok {
		p.mu.Unlock()
		return
	}

	// R4.3: mark draining so pickers stop selecting this SubConn.
	entry.Draining = true
	if entry.HealthCancel != nil {
		entry.HealthCancel()
	}
	// Remove from the live map immediately so Snapshot/HealthyEntries exclude it.
	delete(p.entries, key)
	p.mu.Unlock()


	// R4.7: emit INFO immediately when drain begins.
	p.logger.Info("subconn draining",
		slog.String("endpoint", key.addrPort()),
		slog.String("namespace", key.Namespace),
		slog.String("service", key.Service),
		slog.String("reason", "draining"),
	)

	// R4.2: close the SubConn once in-flight RPCs drain or 5s deadline fires.
	go p.drainAndClose(entry, key)
}

// drainAndClose waits for entry.InFlight to reach zero or for a 5-second
// deadline, then shuts down the SubConn.
func (p *Pool) drainAndClose(entry *SubConnEntry, key PoolKey) {
	const drainTimeout = 5 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()

	// Poll for in-flight count to reach zero.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Deadline exceeded — force close regardless of in-flight count.
			goto done
		case <-ticker.C:
			if entry.InFlight.Load() <= 0 {
				goto done
			}
		}
	}

done:
	entry.SubConn.Shutdown()

	// R4.7: emit INFO log when SubConn is fully closed.
	p.logger.Info("subconn closed",
		slog.String("endpoint", key.addrPort()),
		slog.String("namespace", key.Namespace),
		slog.String("service", key.Service),
		slog.String("reason", "closed"),
	)
}

// UpdateConnState updates the connectivity state of the SubConn identified by
// key.
//
// If the new state is connectivity.TransientFailure, reverifyFn is called to
// ask the Resolver to confirm the backing Endpoint still exists (R4.4).
func (p *Pool) UpdateConnState(key PoolKey, state connectivity.State) {
	p.mu.Lock()
	entry, ok := p.entries[key]
	if !ok {
		p.mu.Unlock()
		return
	}
	entry.ConnState = state
	p.mu.Unlock()

	// R4.4: on TransientFailure, request the Resolver to re-verify the endpoint.
	if state == connectivity.TransientFailure && p.reverifyFn != nil {
		p.reverifyFn(key)
	}
}

// UpdateHealth sets the health state for the SubConn identified by key.
// No-op if the key is not present (e.g., already removed).
func (p *Pool) UpdateHealth(key PoolKey, state HealthState) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if entry, ok := p.entries[key]; ok {
		entry.Health = state
	}
}

// Snapshot returns a slice of all non-draining SubConnEntries for use in
// Picker construction. The returned entries are references to the live state;
// callers that need stable snapshots should copy the relevant fields.
func (p *Pool) Snapshot() []*SubConnEntry {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make([]*SubConnEntry, 0, len(p.entries))
	for _, entry := range p.entries {
		if !entry.Draining {
			result = append(result, entry)
		}
	}
	return result
}

// HealthyEntries returns all non-draining SubConnEntries whose Health state is
// HealthServing. These are the entries eligible for RPC picks.
func (p *Pool) HealthyEntries() []*SubConnEntry {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make([]*SubConnEntry, 0, len(p.entries))
	for _, entry := range p.entries {
		if !entry.Draining && entry.Health == HealthServing {
			result = append(result, entry)
		}
	}
	return result
}
