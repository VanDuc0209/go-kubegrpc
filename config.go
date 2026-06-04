package grpck8sbalancer

import (
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	internalbalancer "github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/balancer"
)

// Config controls Library behavior. All fields have documented defaults (R9.2).
// Use DefaultConfig to obtain a Config pre-populated with those defaults, then
// override individual fields as needed.
type Config struct {
	// DefaultNamespace is the Kubernetes namespace resolved when the Target
	// string omits the namespace segment (R2.2).
	DefaultNamespace string

	// Kubeconfig is the path to a kubeconfig file used to authenticate to the
	// Kubernetes API. When empty the Library uses in-cluster service-account
	// credentials (R3.8, R3.9).
	Kubeconfig string

	// LoadBalancingPolicy selects the active per-RPC policy.
	// Built-in values: "round_robin", "least_request".
	// Custom policies may be registered via RegisterPolicy (R5.2, R5.7).
	LoadBalancingPolicy string

	// EnableHealthCheck controls whether the Health_Checker probes each
	// Subconnection. When false, all Subconnections are treated as healthy and
	// no probe traffic is issued (R6.7).
	EnableHealthCheck bool

	// HealthCheckServiceName is sent as the "service" argument to
	// grpc.health.v1.Health/Check and Watch (R6.2).
	HealthCheckServiceName string

	// HealthCheckInterval is the period between periodic Check calls when the
	// backend does not support streaming Watch (R6.8).
	HealthCheckInterval time.Duration

	// HealthCheckTimeout is the deadline applied to each individual probe
	// attempt. Exceeding it marks the Subconnection unhealthy (R6.6).
	HealthCheckTimeout time.Duration

	// RetryPolicy configures retry and backoff behaviour (R7.1).
	RetryPolicy RetryPolicy

	// ResolverGraceWindow is how long the Library retains the last-known
	// Endpoint set while the Kubernetes API is unreachable before issuing a
	// WARN log (R8.5).
	ResolverGraceWindow time.Duration

	// MaxSubconnections limits the number of Subconnections maintained per
	// Client. 0 means unbounded (R12.1, R12.2).
	MaxSubconnections int

	// DialOptions are applied to every Subconnection in addition to the
	// Library's internal options (R9.5).
	DialOptions []grpc.DialOption

	// Logger is the structured logger used by all Library components. When nil
	// the Library uses slog.Default() (R9.6).
	Logger *slog.Logger

	// MeterProvider is the OTel MeterProvider used to create instruments. When
	// nil the Library uses the global OTel provider (R10.4).
	MeterProvider metric.MeterProvider
}

// RetryPolicy configures retry and exponential-backoff behaviour (R7.1, R7.4).
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts (initial + retries).
	// Must be >= 1. Use 1 to disable retry (R7.8).
	MaxAttempts int

	// InitialBackoff is the sleep duration before the second attempt (R7.4).
	InitialBackoff time.Duration

	// MaxBackoff is the upper bound on the computed backoff duration (R7.4).
	MaxBackoff time.Duration

	// BackoffMultiplier is the factor applied to InitialBackoff on each
	// subsequent attempt. Must be >= 1.0 (R7.4).
	BackoffMultiplier float64

	// RetryableStatusCodes is the set of gRPC status codes that trigger a
	// retry. Must be non-nil and non-empty when MaxAttempts > 1 (R7.2, R7.6).
	RetryableStatusCodes []codes.Code
}

// DefaultConfig returns a Config pre-populated with the documented default
// values (R9.3). Callers may override individual fields after calling this
// function.
func DefaultConfig() *Config {
	return &Config{
		LoadBalancingPolicy: "round_robin",
		EnableHealthCheck:   true,
		HealthCheckInterval: 10 * time.Second,
		HealthCheckTimeout:  2 * time.Second,
		RetryPolicy: RetryPolicy{
			MaxAttempts:       3,
			InitialBackoff:    100 * time.Millisecond,
			MaxBackoff:        2 * time.Second,
			BackoffMultiplier: 2.0,
			RetryableStatusCodes: []codes.Code{
				codes.Unavailable,
				codes.ResourceExhausted,
				codes.Aborted,
			},
		},
		ResolverGraceWindow: 30 * time.Second,
		MaxSubconnections:   0, // unbounded
		// Logger: nil  → slog.Default() at use-site
		// MeterProvider: nil → global OTel provider at use-site
	}
}

// validate checks that every field of c is within its documented valid range.
// Returned errors name the offending field and the violated bound (R9.4).
func (c *Config) validate() error {
	// LoadBalancingPolicy must resolve to a registered policy in the internal
	// registry (covers both built-ins and custom-registered policies).
	if _, err := internalbalancer.LookupPolicy(c.LoadBalancingPolicy); err != nil {
		known := internalbalancer.KnownPolicies()
		return fmt.Errorf(
			"Config.LoadBalancingPolicy: unknown policy %q (known: %v): %w",
			c.LoadBalancingPolicy,
			known,
			ErrUnknownPolicy,
		)
	}

	// RetryPolicy.MaxAttempts must be >= 1.
	if c.RetryPolicy.MaxAttempts < 1 {
		return fmt.Errorf(
			"Config.RetryPolicy.MaxAttempts: must be >= 1, got %d",
			c.RetryPolicy.MaxAttempts,
		)
	}

	// RetryPolicy.InitialBackoff must be non-negative.
	if c.RetryPolicy.InitialBackoff < 0 {
		return fmt.Errorf(
			"Config.RetryPolicy.InitialBackoff: must be >= 0, got %s",
			c.RetryPolicy.InitialBackoff,
		)
	}

	// RetryPolicy.MaxBackoff must be non-negative.
	if c.RetryPolicy.MaxBackoff < 0 {
		return fmt.Errorf(
			"Config.RetryPolicy.MaxBackoff: must be >= 0, got %s",
			c.RetryPolicy.MaxBackoff,
		)
	}

	// RetryPolicy.BackoffMultiplier must be >= 1.0.
	if c.RetryPolicy.BackoffMultiplier < 1.0 {
		return fmt.Errorf(
			"Config.RetryPolicy.BackoffMultiplier: must be >= 1.0, got %g",
			c.RetryPolicy.BackoffMultiplier,
		)
	}

	// MaxSubconnections must be non-negative (0 = unbounded).
	if c.MaxSubconnections < 0 {
		return fmt.Errorf(
			"Config.MaxSubconnections: must be >= 0, got %d",
			c.MaxSubconnections,
		)
	}

	// When MaxAttempts > 1, RetryableStatusCodes must be non-nil and non-empty.
	if c.RetryPolicy.MaxAttempts > 1 && len(c.RetryPolicy.RetryableStatusCodes) == 0 {
		return fmt.Errorf(
			"Config.RetryPolicy.RetryableStatusCodes: must be non-empty when MaxAttempts > 1 (MaxAttempts=%d)",
			c.RetryPolicy.MaxAttempts,
		)
	}

	// HealthCheckInterval must be non-negative.
	if c.HealthCheckInterval < 0 {
		return fmt.Errorf(
			"Config.HealthCheckInterval: must be >= 0, got %s",
			c.HealthCheckInterval,
		)
	}

	// HealthCheckTimeout must be non-negative.
	if c.HealthCheckTimeout < 0 {
		return fmt.Errorf(
			"Config.HealthCheckTimeout: must be >= 0, got %s",
			c.HealthCheckTimeout,
		)
	}

	// ResolverGraceWindow must be non-negative.
	if c.ResolverGraceWindow < 0 {
		return fmt.Errorf(
			"Config.ResolverGraceWindow: must be >= 0, got %s",
			c.ResolverGraceWindow,
		)
	}

	return nil
}


