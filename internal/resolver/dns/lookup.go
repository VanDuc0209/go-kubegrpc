package dns

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/resolver"
)

const resolveInterval = 30 * time.Second

// dnsResolver implements resolver.Resolver using periodic DNS A/AAAA lookups.
type dnsResolver struct {
	host     string
	port     int
	lookupFn LookupFunc

	closeOnce sync.Once
	cancel    context.CancelFunc
	done      chan struct{}
}

// newDNSResolver creates a dnsResolver with the given host, numeric port, and
// lookup function. The resolver is not started until Start is called.
func newDNSResolver(host string, port int, lookupFn LookupFunc) *dnsResolver {
	return &dnsResolver{
		host:     host,
		port:     port,
		lookupFn: lookupFn,
		done:     make(chan struct{}),
	}
}

// Start launches the lookup goroutine. It performs an initial lookup
// immediately, then repeats every 30 seconds. The goroutine exits when ctx is
// canceled or Close is called. Start must not block; it returns immediately
// after spawning the goroutine.
func (r *dnsResolver) Start(ctx context.Context, fn func(resolver.Snapshot)) error {
	// Derive a cancelable child context so Close() can stop the loop
	// independently of the parent context.
	loopCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	go r.loop(loopCtx, fn)
	return nil
}

// Close stops the resolver. Safe to call multiple times; subsequent calls are
// no-ops.
func (r *dnsResolver) Close() error {
	r.closeOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		<-r.done
	})
	return nil
}

// loop performs the periodic DNS lookup until the context is canceled.
func (r *dnsResolver) loop(ctx context.Context, fn func(resolver.Snapshot)) {
	defer close(r.done)

	// Perform the initial lookup immediately before starting the ticker.
	r.resolve(ctx, fn)

	ticker := time.NewTicker(resolveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.resolve(ctx, fn)
		}
	}
}

// resolve performs a single DNS lookup and calls fn with the resulting Snapshot.
func (r *dnsResolver) resolve(ctx context.Context, fn func(resolver.Snapshot)) {
	addrs, err := r.lookupFn(ctx, r.host)
	if err != nil {
		fn(resolver.Snapshot{
			Err:   fmt.Errorf("dns resolver: lookup %q: %w", r.host, err),
			State: resolver.ResolverDisconnected,
		})
		return
	}

	endpoints := make([]resolver.Endpoint, 0, len(addrs))
	for _, addr := range addrs {
		endpoints = append(endpoints, resolver.Endpoint{
			Address: addr.String(),
			Port:    r.port,
			Ready:   true,
		})
	}

	fn(resolver.Snapshot{
		Endpoints: endpoints,
		State:     resolver.ResolverConnected,
	})
}
