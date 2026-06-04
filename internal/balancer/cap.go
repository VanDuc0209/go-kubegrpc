package balancer

import (
	"fmt"
	"sort"

	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/resolver"
)

// CapEndpoints applies the MaxSubconnections cap to a desired endpoint set.
// When cap > 0, it returns the first `cap` endpoints in deterministic
// lexicographic order of "address:port". When cap <= 0, returns endpoints as-is.
//
// The returned slice is a new allocation; the input is not modified.
// Requirements: 12.1, 12.2
func CapEndpoints(endpoints []resolver.Endpoint, maxSubconns int) []resolver.Endpoint {
	if maxSubconns <= 0 {
		// No cap — return a copy.
		result := make([]resolver.Endpoint, len(endpoints))
		copy(result, endpoints)
		return result
	}

	// Sort a copy by "address:port" lexicographically.
	sorted := make([]resolver.Endpoint, len(endpoints))
	copy(sorted, endpoints)
	sort.Slice(sorted, func(i, j int) bool {
		ai := fmt.Sprintf("%s:%d", sorted[i].Address, sorted[i].Port)
		aj := fmt.Sprintf("%s:%d", sorted[j].Address, sorted[j].Port)
		return ai < aj
	})

	if len(sorted) > maxSubconns {
		return sorted[:maxSubconns]
	}
	return sorted
}

// EndpointsToMap converts a slice of Endpoints to a map keyed by the
// pool Key, for use in reconcile set-difference operations.
// The namespace and service parameters are used to build the full PoolKey.
func EndpointsToMap(namespace, service string, endpoints []resolver.Endpoint) map[resolver.Key]resolver.Endpoint {
	m := make(map[resolver.Key]resolver.Endpoint, len(endpoints))
	for _, ep := range endpoints {
		m[resolver.KeyOf(ep)] = ep
	}
	return m
}
