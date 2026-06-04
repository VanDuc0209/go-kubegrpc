package balancer

import (
	"fmt"

	grpcbalancer "google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// lrPicker implements least-request load balancing.
//
// Property P7: the SubConn selected has the minimum InFlight among all healthy
// SubConns. Ties are broken by lexicographic order of "address:port" to ensure
// determinism across concurrent callers.
//
// InFlight is incremented atomically on the chosen SubConn at pick time and
// decremented by the Done callback returned in PickResult, satisfying R5.5.
type lrPicker struct {
	healthy []*SubConnEntry
}

// newLRPicker constructs an lrPicker from the current set of healthy SubConns.
// It satisfies the PickerFactory signature so it can be stored in the registry.
func newLRPicker(healthy []*SubConnEntry) grpcbalancer.Picker {
	return &lrPicker{healthy: healthy}
}

// Pick selects the healthy SubConn with the smallest InFlight count.
// On ties, the SubConn with the lexicographically smaller "address:port" string
// is chosen to make the selection deterministic (R5.5).
//
// Returns codes.Unavailable when the healthy set is empty (R5.6).
func (p *lrPicker) Pick(info grpcbalancer.PickInfo) (grpcbalancer.PickResult, error) {
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

	var chosen *SubConnEntry
	var chosenKey string
	var chosenInFlight int64

	for _, entry := range candidates {
		inFlight := entry.InFlight.Load()
		key := fmt.Sprintf("%s:%d", entry.Key.Address, entry.Key.Port)

		if chosen == nil ||
			inFlight < chosenInFlight ||
			(inFlight == chosenInFlight && key < chosenKey) {
			chosen = entry
			chosenKey = key
			chosenInFlight = inFlight
		}
	}

	// Increment in-flight count before returning so that concurrent pickers
	// see updated counters when making their own selections (R5.5).
	chosen.InFlight.Add(1)

	// Record chosen address in tracker.
	if info.Ctx != nil {
		if val := info.Ctx.Value(TrackerKey); val != nil {
			if tracker, ok := val.(*AttemptTracker); ok {
				tracker.Mu.Lock()
				tracker.Addrs = append(tracker.Addrs, chosen.Key.addrPort())
				tracker.Mu.Unlock()
			}
		}
	}

	return grpcbalancer.PickResult{
		SubConn: chosen.SubConn,
		Done: func(_ grpcbalancer.DoneInfo) {
			// Decrement in-flight when the RPC completes (R5.5).
			chosen.InFlight.Add(-1)
		},
	}, nil
}

