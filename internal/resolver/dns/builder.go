package dns

import (
	"context"
	"net"

	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/resolver"
)

// LookupFunc is the signature for DNS A/AAAA record lookup.
// It is injectable for testing.
type LookupFunc func(ctx context.Context, host string) ([]net.IPAddr, error)

// Builder creates dnsResolver instances.
type Builder struct {
	// LookupFn overrides net.DefaultResolver.LookupIPAddr (injectable for tests).
	LookupFn LookupFunc
}

// NewResolver creates a new DNS resolver for the given host and numeric port.
// If the Builder's LookupFn is nil, net.DefaultResolver.LookupIPAddr is used.
func (b *Builder) NewResolver(host string, port int) resolver.Resolver {
	fn := b.LookupFn
	if fn == nil {
		fn = net.DefaultResolver.LookupIPAddr
	}
	return newDNSResolver(host, port, fn)
}
