// Package grpck8sbalancer provides a Kubernetes-native gRPC client with
// client-side load balancing, dynamic service discovery, health checking,
// retry, and connection lifecycle management.
//
// The only type application code should use is [Client], obtained via [NewClient].
package grpck8sbalancer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	internalbalancer "github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/balancer"
	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/retry"
)

// Client is the only type application code uses to invoke RPCs.
// It is safe for concurrent use by multiple goroutines (R11.1).
type Client interface {
	grpc.ClientConnInterface
	Close() error // R1.4, R1.5, R1.6, R12.3
}

// client wraps a *grpc.ClientConn with lifecycle management.
type client struct {
	conn      *grpc.ClientConn
	closeOnce sync.Once
	closed    atomic.Bool // true after Close()
	wg        sync.WaitGroup
	cancel    context.CancelFunc
}

// Compile-time interface check (R1.2).
var _ grpc.ClientConnInterface = (*client)(nil)

// NewClient constructs a Client for the given target string and optional
// configuration. If cfg is nil, DefaultConfig() is used (R9.2).
//
// Errors are returned for:
//   - nil or invalid Config fields (R9.4)
//   - malformed target string (R2.2, R2.3, R2.6)
//   - unrecognised LoadBalancingPolicy (R5.3)
//
// On any error, no goroutines or connections are leaked (R1.3).
func NewClient(ctx context.Context, target string, cfg *Config) (Client, error) {
	// R9.2: apply defaults when no config is supplied.
	if cfg == nil {
		cfg = DefaultConfig()
	}

	// R9.4: validate every field before touching gRPC internals.
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("NewClient: %w", err)
	}

	// R2.2, R2.3: parse and validate the target string.
	t, err := ParseTarget(target, cfg.DefaultNamespace)
	if err != nil {
		return nil, fmt.Errorf("NewClient: %w", err)
	}

	// R5.3: confirm the policy is registered before building the connection.
	if _, err := internalbalancer.LookupPolicy(cfg.LoadBalancingPolicy); err != nil {
		return nil, fmt.Errorf("NewClient: %w", ErrUnknownPolicy)
	}

	// Provide the balancer builder with the runtime configuration it needs to
	// select the right Picker factory and apply the MaxSubconnections cap.
	internalbalancer.SetActiveConfig(&internalbalancer.BuildConfig{
		Namespace:              t.Namespace,
		Service:                t.Service,
		Policy:                 cfg.LoadBalancingPolicy,
		MaxSubconns:            cfg.MaxSubconnections,
		Logger:                 cfg.Logger,
		EnableHealthCheck:      cfg.EnableHealthCheck,
		HealthCheckServiceName: cfg.HealthCheckServiceName,
		HealthCheckInterval:    cfg.HealthCheckInterval,
		HealthCheckTimeout:     cfg.HealthCheckTimeout,
	})


	// Build the retry policy value used by the interceptors.
	retryPolicy := retry.Policy{
		MaxAttempts:          cfg.RetryPolicy.MaxAttempts,
		InitialBackoff:       cfg.RetryPolicy.InitialBackoff,
		MaxBackoff:           cfg.RetryPolicy.MaxBackoff,
		BackoffMultiplier:    cfg.RetryPolicy.BackoffMultiplier,
		RetryableStatusCodes: cfg.RetryPolicy.RetryableStatusCodes,
	}

	// Construct retry interceptors; pass IsNonIdempotent to avoid an import
	// cycle between internal/retry and the root package.
	retryUnary := retry.UnaryClientInterceptor(retryPolicy, IsNonIdempotent, nil)
	retryStream := retry.StreamClientInterceptor(retryPolicy, IsNonIdempotent, nil)

	// Build the service-config JSON that tells gRPC to use our custom balancer.
	svcCfgJSON, err := buildServiceConfigJSON()
	if err != nil {
		// json.Marshal of a static map should never fail, but handle it anyway.
		return nil, fmt.Errorf("NewClient: failed to build service config: %w", err)
	}

	// Instantiate dedicated resolver builders for this connection (Task 1).
	k8sBuilder := &k8sResolverBuilder{cfg: cfg, t: t}
	dnsBuilder := &dnsResolverBuilder{cfg: cfg, t: t}

	// Assemble dial options: library-internal options first, then caller-supplied
	// options so that the caller can still override transport credentials etc.
	dialOpts := []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(retryUnary),
		grpc.WithChainStreamInterceptor(retryStream),
		grpc.WithDefaultServiceConfig(svcCfgJSON),
		grpc.WithResolvers(k8sBuilder, dnsBuilder),
		// Default to insecure transport. Callers that need TLS should pass
		// grpc.WithTransportCredentials(...) inside cfg.DialOptions, which
		// will override this entry through gRPC's last-wins semantics.
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	dialOpts = append(dialOpts, cfg.DialOptions...)


	// Derive a cancellable child context to control background goroutines.
	_, cancel := context.WithCancel(ctx)

	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("NewClient: grpc.NewClient: %w", err)
	}

	return &client{
		conn:   conn,
		cancel: cancel,
	}, nil
}

// Close releases all Subconnections, stops background goroutines, and
// renders this Client unusable. The method is idempotent: subsequent calls
// return nil without side effects (R1.4, R1.5, R1.6, R12.3).
func (c *client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		// Mark the client as closed so in-flight Invoke / NewStream calls
		// that have not yet reached the transport can return ErrClientClosed.
		c.closed.Store(true)

		// Cancel the root context to signal all goroutines to stop.
		c.cancel()

		// Wait for all spawned goroutines with a 5-second hard deadline (R12.3).
		done := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			// Goroutines did not exit in time; proceed to close the connection
			// anyway to avoid leaking the underlying transport.
		}

		err = c.conn.Close()
	})
	return err
}

// Invoke executes a unary RPC. Returns ErrClientClosed when the Client has
// already been closed (R11.1).
func (c *client) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	if c.closed.Load() {
		return ErrClientClosed
	}
	return c.conn.Invoke(ctx, method, args, reply, opts...)
}

// NewStream opens a streaming RPC. Returns ErrClientClosed when the Client
// has already been closed (R11.1).
func (c *client) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if c.closed.Load() {
		return nil, ErrClientClosed
	}
	return c.conn.NewStream(ctx, desc, method, opts...)
}

// RegisterPolicy registers a custom load-balancing policy under name so that
// it can be selected via Config.LoadBalancingPolicy (R5.7).
//
// Returns ErrPolicyExists (wrapped) when name is already taken.
func RegisterPolicy(name string, factory internalbalancer.PickerFactory) error {
	return internalbalancer.RegisterPolicy(name, factory)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// serviceConfigJSON is the static payload that instructs gRPC to use the
// k8s_balancer balancer builder registered by internal/balancer.
type serviceConfigJSON struct {
	LoadBalancingConfig []map[string]any `json:"loadBalancingConfig"`
}

// buildServiceConfigJSON returns the JSON string for the gRPC service config
// that selects the k8s_balancer balancer.
func buildServiceConfigJSON() (string, error) {
	cfg := serviceConfigJSON{
		LoadBalancingConfig: []map[string]any{
			{internalbalancer.BalancerName: map[string]any{}},
		},
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
