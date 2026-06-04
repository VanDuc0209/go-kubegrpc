// Package resolver defines the scheme-agnostic Resolver interface and the
// associated data types shared by both the k8s:// and dns:// resolver
// implementations.
package resolver

import "context"

// ResolverState represents the current connection state of the resolver.
type ResolverState int

const (
	// ResolverConnected indicates the resolver has an active connection to its
	// discovery backend (Kubernetes API server or DNS) and is emitting live
	// endpoint snapshots.
	ResolverConnected ResolverState = iota

	// ResolverDisconnected indicates the resolver has lost its connection to
	// the discovery backend and is attempting to reconnect with exponential
	// backoff (R3.5).
	ResolverDisconnected

	// ResolverInGraceWindow indicates the resolver has been disconnected for
	// longer than Config.ResolverGraceWindow. The last-known endpoint set is
	// retained and continues to be used until connectivity is restored (R8.5).
	ResolverInGraceWindow
)

// String returns a human-readable name for the ResolverState.
func (s ResolverState) String() string {
	switch s {
	case ResolverConnected:
		return "Connected"
	case ResolverDisconnected:
		return "Disconnected"
	case ResolverInGraceWindow:
		return "InGraceWindow"
	default:
		return "Unknown"
	}
}

// Endpoint is a single backend endpoint discovered by the resolver.
// Only endpoints that are ready and non-terminating are included in a
// Snapshot (R3.2, R3.3).
type Endpoint struct {
	// Address is the host or IP of the backend pod.
	Address string

	// Port is the resolved numeric port for this endpoint.
	Port int

	// Ready indicates the endpoint is ready to serve traffic. The resolver
	// only includes endpoints where Ready is true in Snapshots (R3.2, R3.3).
	Ready bool
}

// Key uniquely identifies an endpoint within a resolver result.
// It is used by the reconcile algorithm to compute set differences between
// consecutive Snapshots (add/remove deltas) keyed on (address, port) identity.
type Key struct {
	Address string
	Port    int
}

// KeyOf returns the Key for an Endpoint, extracting the (address, port) pair
// that serves as its unique identity within the pool (R4.5).
func KeyOf(e Endpoint) Key {
	return Key{
		Address: e.Address,
		Port:    e.Port,
	}
}

// Snapshot is a full set of currently-known endpoints emitted by the resolver.
// Each Snapshot replaces the previous one; reconciliation logic derives add and
// remove deltas from consecutive Snapshots by computing set differences on Keys.
type Snapshot struct {
	// Endpoints is the complete set of ready, non-terminating endpoints known
	// at the time the Snapshot was created.
	Endpoints []Endpoint

	// State is the resolver's current connection state at the time of emission.
	State ResolverState

	// Err is non-nil when an error caused a state transition (e.g., a watch
	// stream closed unexpectedly). The resolver emits
	// Snapshot{Err: err, State: ResolverDisconnected} on such errors.
	Err error
}

// Resolver is the internal interface implemented by both the k8s and dns
// resolvers. It produces a stream of Endpoint Snapshots via a callback so that
// the balancer layer remains scheme-agnostic.
type Resolver interface {
	// Start begins resolution and pushes Snapshots to fn until ctx is canceled
	// or Close is called. Errors are reported as
	// Snapshot{Err: err, State: ResolverDisconnected}. Start must not block; it
	// launches a background goroutine and returns immediately.
	Start(ctx context.Context, fn func(Snapshot)) error

	// Close stops the resolver and releases all associated resources.
	// It is safe to call Close multiple times; subsequent calls are no-ops.
	Close() error
}
