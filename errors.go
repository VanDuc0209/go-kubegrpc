package grpck8sbalancer

import "errors"

// Sentinel errors returned by the Library. Callers may use errors.Is to
// distinguish them from one another. At call-sites within the library these
// are wrapped with %w so that context is preserved while identity is kept.
var (
	// ErrTargetInvalid is returned when the target string cannot be parsed.
	ErrTargetInvalid = errors.New("grpc-k8s-balancer: invalid target")

	// ErrUnsupportedScheme is returned when the target uses a scheme other
	// than "k8s" or "dns".
	ErrUnsupportedScheme = errors.New("grpc-k8s-balancer: unsupported scheme")

	// ErrMissingNamespace is returned when the target omits the namespace and
	// Config.DefaultNamespace is also empty.
	ErrMissingNamespace = errors.New("grpc-k8s-balancer: missing namespace")

	// ErrUnknownPolicy is returned when Config.LoadBalancingPolicy names a
	// policy that has not been registered.
	ErrUnknownPolicy = errors.New("grpc-k8s-balancer: unknown load balancing policy")

	// ErrPolicyExists is returned by RegisterPolicy when the given name has
	// already been registered.
	ErrPolicyExists = errors.New("grpc-k8s-balancer: policy already registered")

	// ErrNoHealthySubconns is returned when an RPC is attempted but there are
	// no healthy subconnections available to serve it.
	ErrNoHealthySubconns = errors.New("grpc-k8s-balancer: no healthy subconnections")

	// ErrClientClosed is returned when Invoke or NewStream is called after
	// Close has been called on the Client.
	ErrClientClosed = errors.New("grpc-k8s-balancer: client is closed")
)
