package grpck8sbalancer

import "google.golang.org/grpc"

// nonIdempotentKey is the private marker type that NonIdempotent embeds.
// The retry interceptor detects this type via type assertion.
type nonIdempotentKey struct {
	grpc.EmptyCallOption
}

// NonIdempotent returns a per-call grpc.CallOption that marks the outgoing RPC
// as non-idempotent. When this option is present, the retry coordinator will
// not retry the RPC regardless of the status code (R7.7).
//
// Usage:
//
//	client.Invoke(ctx, method, in, out, grpck8sbalancer.NonIdempotent())
func NonIdempotent() grpc.CallOption {
	return nonIdempotentKey{}
}

// IsNonIdempotent reports whether any element of opts is a NonIdempotent marker.
// This helper is used by the retry interceptor in internal/retry.
func IsNonIdempotent(opts []grpc.CallOption) bool {
	for _, o := range opts {
		if _, ok := o.(nonIdempotentKey); ok {
			return true
		}
	}
	return false
}
