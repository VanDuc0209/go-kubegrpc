package grpck8sbalancer

import (
	"context"
	"errors"
	"testing"
)

// TestNewClientInvalidTarget verifies that a target with an unsupported scheme
// returns a non-nil error wrapping ErrUnsupportedScheme (R1.3, R2.6).
func TestNewClientInvalidTarget(t *testing.T) {
	ctx := context.Background()

	c, err := NewClient(ctx, "static://host:8080", nil)
	if err == nil {
		_ = c.Close()
		t.Fatal("expected error for unsupported scheme, got nil")
	}
	if c != nil {
		t.Errorf("expected nil Client on error, got non-nil")
	}
	if !errors.Is(err, ErrUnsupportedScheme) {
		t.Errorf("expected errors.Is(err, ErrUnsupportedScheme) to be true; err=%v", err)
	}
}

// TestNewClientMissingNamespace verifies that a k8s target without a namespace
// segment and an empty DefaultNamespace returns an error wrapping
// ErrMissingNamespace (R1.3, R2.3).
func TestNewClientMissingNamespace(t *testing.T) {
	ctx := context.Background()

	cfg := DefaultConfig()
	cfg.DefaultNamespace = ""

	c, err := NewClient(ctx, "k8s:///service:8080", cfg)
	if err == nil {
		_ = c.Close()
		t.Fatal("expected error for missing namespace, got nil")
	}
	if c != nil {
		t.Errorf("expected nil Client on error, got non-nil")
	}
	if !errors.Is(err, ErrMissingNamespace) {
		t.Errorf("expected errors.Is(err, ErrMissingNamespace) to be true; err=%v", err)
	}
}

// TestNewClientUnknownPolicy verifies that an unrecognised LoadBalancingPolicy
// causes NewClient to return a non-nil error that references the policy field
// and wraps ErrUnknownPolicy (R5.3).
func TestNewClientUnknownPolicy(t *testing.T) {
	ctx := context.Background()

	cfg := DefaultConfig()
	cfg.LoadBalancingPolicy = "nonexistent_policy_xyz"

	c, err := NewClient(ctx, "dns:///host:8080", cfg)
	if err == nil {
		_ = c.Close()
		t.Fatal("expected error for unknown load balancing policy, got nil")
	}
	if c != nil {
		t.Errorf("expected nil Client on error, got non-nil")
	}
	if !errors.Is(err, ErrUnknownPolicy) {
		t.Errorf("expected errors.Is(err, ErrUnknownPolicy) to be true; err=%v", err)
	}
}

// TestClientInterfaceCompliance verifies that NewClient returns a non-nil
// Client for a valid DNS target and that the returned value satisfies the
// Client interface (R1.2). grpc.NewClient is lazy — no server needs to be
// running for this call to succeed.
func TestClientInterfaceCompliance(t *testing.T) {
	ctx := context.Background()

	c, err := NewClient(ctx, "dns:///localhost:12345", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil Client, got nil")
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close() returned unexpected error: %v", err)
	}
}

// TestCloseIdempotent verifies that calling Close twice on the same Client
// returns nil both times and does not panic (R1.6).
func TestCloseIdempotent(t *testing.T) {
	ctx := context.Background()

	c, err := NewClient(ctx, "dns:///localhost:12345", nil)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	if err := c.Close(); err != nil {
		t.Errorf("first Close() returned unexpected error: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close() returned unexpected error: %v", err)
	}
}
