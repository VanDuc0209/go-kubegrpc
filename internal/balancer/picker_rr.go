package balancer

import (
	"sync/atomic"

	grpcbalancer "google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rrPicker implements round-robin load balancing using an atomic counter.
//
// Property P3: for N healthy SubConns, N consecutive picks form a permutation
// (i.e., every SubConn is selected exactly once before any is repeated).
// This holds because counter increments modulo N cycle through all indices.
//
// The picker is immutable after construction; the counter is the only mutable
// field and is accessed exclusively through atomic operations so that concurrent
// Pick calls are safe without additional synchronisation.
type rrPicker struct {
	healthy []*SubConnEntry
	counter atomic.Uint64
}

// newRRPicker constructs a rrPicker from the current set of healthy SubConns.
// It satisfies the PickerFactory signature so it can be stored in the registry.
func newRRPicker(healthy []*SubConnEntry) grpcbalancer.Picker {
	return &rrPicker{healthy: healthy}
}

// Pick selects the next SubConn using a monotonically increasing counter
// modulo the number of healthy SubConns, satisfying Requirements 5.1 and 5.4.
//
// Returns codes.Unavailable when the healthy set is empty (R5.6).
func (p *rrPicker) Pick(info grpcbalancer.PickInfo) (grpcbalancer.PickResult, error) {
	if len(p.healthy) == 0 {
		return grpcbalancer.PickResult{}, status.Error(codes.Unavailable, "no healthy subconns")
	}

	candidates := p.healthy

	// Apply attempt exclusion if tracker is present and alternatives exist (R7.3).
	if info.Ctx != nil {
		if val := info.Ctx.Value(TrackerKey); val != nil {
			if tracker, ok := val.(*AttemptTracker); ok {
				tracker.Mu.Lock()
				tried := tracker.Addrs
				tracker.Mu.Unlock()

				if len(tried) > 0 {
					// Filter out tried candidates if we have at least one alternative healthy subconnection.
					filtered := make([]*SubConnEntry, 0, len(p.healthy))
					for _, entry := range p.healthy {
						isTried := false
						addr := entry.Key.addrPort()
						for _, tAddr := range tried {
							if addr == tAddr {
								isTried = true
								break
							}
						}
						if !isTried {
							filtered = append(filtered, entry)
						}
					}
					if len(filtered) > 0 {
						candidates = filtered
					}
				}
			}
		}
	}

	// Add(1) returns the incremented value; mod N maps it to a valid index.
	idx := p.counter.Add(1) % uint64(len(candidates))
	entry := candidates[idx]

	// Record chosen address in tracker.
	if info.Ctx != nil {
		if val := info.Ctx.Value(TrackerKey); val != nil {
			if tracker, ok := val.(*AttemptTracker); ok {
				tracker.Mu.Lock()
				tracker.Addrs = append(tracker.Addrs, entry.Key.addrPort())
				tracker.Mu.Unlock()
			}
		}
	}

	return grpcbalancer.PickResult{SubConn: entry.SubConn}, nil
}

