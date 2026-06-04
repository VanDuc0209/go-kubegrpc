package dns

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/resolver"
)

// TestDNSResolverEmitsSnapshot verifies that the DNS resolver emits a snapshot
// containing both expected endpoints when the lookup function returns two IPs.
// Requirements: 3.5
func TestDNSResolverEmitsSnapshot(t *testing.T) {
	t.Parallel()

	ip1 := net.IPAddr{IP: net.ParseIP("10.0.0.1")}
	ip2 := net.IPAddr{IP: net.ParseIP("10.0.0.2")}

	fakeLookup := func(_ context.Context, _ string) ([]net.IPAddr, error) {
		return []net.IPAddr{ip1, ip2}, nil
	}

	b := &Builder{LookupFn: fakeLookup}
	r := b.NewResolver("example.internal", 8080)

	snapshots := make(chan resolver.Snapshot, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := r.Start(ctx, func(s resolver.Snapshot) {
		snapshots <- s
	}); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}
	defer r.Close() //nolint:errcheck

	select {
	case snap := <-snapshots:
		if snap.State != resolver.ResolverConnected {
			t.Errorf("expected State=ResolverConnected, got %v", snap.State)
		}
		if snap.Err != nil {
			t.Errorf("expected nil Err, got %v", snap.Err)
		}
		if len(snap.Endpoints) != 2 {
			t.Fatalf("expected 2 endpoints, got %d", len(snap.Endpoints))
		}
		// Collect addresses and verify both IPs are present.
		addrs := make(map[string]bool, 2)
		for _, ep := range snap.Endpoints {
			addrs[ep.Address] = true
			if ep.Port != 8080 {
				t.Errorf("expected port 8080, got %d", ep.Port)
			}
			if !ep.Ready {
				t.Errorf("expected endpoint to be Ready")
			}
		}
		if !addrs[ip1.String()] {
			t.Errorf("missing endpoint for %s", ip1.String())
		}
		if !addrs[ip2.String()] {
			t.Errorf("missing endpoint for %s", ip2.String())
		}

	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for initial snapshot")
	}
}

// TestDNSResolverHandlesErrors verifies that a lookup error is propagated as a
// Snapshot with a non-nil Err and State==ResolverDisconnected, and that the
// resolver does not panic.
// Requirements: 3.5
func TestDNSResolverHandlesErrors(t *testing.T) {
	t.Parallel()

	lookupErr := errors.New("dns: temporary failure")

	fakeLookup := func(_ context.Context, _ string) ([]net.IPAddr, error) {
		return nil, lookupErr
	}

	b := &Builder{LookupFn: fakeLookup}
	r := b.NewResolver("broken.internal", 9090)

	snapshots := make(chan resolver.Snapshot, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := r.Start(ctx, func(s resolver.Snapshot) {
		snapshots <- s
	}); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}
	defer r.Close() //nolint:errcheck

	select {
	case snap := <-snapshots:
		if snap.State != resolver.ResolverDisconnected {
			t.Errorf("expected State=ResolverDisconnected, got %v", snap.State)
		}
		if snap.Err == nil {
			t.Error("expected non-nil Err in error snapshot")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for error snapshot")
	}
}

// TestDNSResolverStopsOnContextCancel verifies that the resolver stops promptly
// when the context is canceled (i.e., Close returns without deadlocking).
// Requirements: 3.5
func TestDNSResolverStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	// Use a slow lookup so the resolver is idle in the select loop, not in a call.
	fakeLookup := func(_ context.Context, _ string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("192.168.1.1")}}, nil
	}

	b := &Builder{LookupFn: fakeLookup}
	r := b.NewResolver("stop.internal", 7070)

	ctx, cancel := context.WithCancel(context.Background())

	snapshots := make(chan resolver.Snapshot, 4)
	if err := r.Start(ctx, func(s resolver.Snapshot) {
		snapshots <- s
	}); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}

	// Wait for the initial snapshot so the resolver goroutine is running.
	select {
	case <-snapshots:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for initial snapshot before cancel")
	}

	// Cancel the context and then verify Close returns promptly.
	cancel()

	done := make(chan struct{})
	go func() {
		r.Close() //nolint:errcheck
		close(done)
	}()

	select {
	case <-done:
		// Close returned promptly — resolver stopped as expected.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("resolver did not stop within 500ms after context cancel (possible deadlock)")
	}
}
