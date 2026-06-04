package health

import (
	"context"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HealthState represents the health status of a subconnection.
type HealthState int

const (
	// HealthUnknown is the initial state before any probe result is available.
	HealthUnknown HealthState = iota
	// HealthServing indicates the backend is healthy and ready to serve traffic.
	HealthServing
	// HealthNotServing indicates the backend is unhealthy and should not receive traffic.
	HealthNotServing
)

// CheckerConfig holds configuration parameters for the health checker.
type CheckerConfig struct {
	// ServiceName is the gRPC service name sent as the "service" field in health probes (R6.2).
	ServiceName string
	// Interval controls how often periodic Check RPCs are issued when Watch is unsupported (R6.8).
	Interval time.Duration
	// Timeout is applied as a per-probe deadline to each Check call (R6.6).
	Timeout time.Duration
	// EnableCheck controls whether health probing is active. When false, all subconnections
	// are treated as healthy immediately without issuing any probes (R6.7).
	EnableCheck bool
}

// Checker defines the interface for health checking a subconnection.
type Checker interface {
	// Watch starts health probing for the given ClientConn. It returns a channel that emits
	// HealthState transitions. The channel is closed when ctx is canceled. Satisfies R6.1.
	Watch(ctx context.Context, conn *grpc.ClientConn) (<-chan HealthState, error)
}

// DefaultChecker is the default implementation of Checker. It probes backends using the
// gRPC Health Checking Protocol, falling back to periodic Check RPCs when Watch is
// unsupported (R6.8).
type DefaultChecker struct {
	cfg CheckerConfig
}

// NewChecker constructs a DefaultChecker with the provided configuration.
func NewChecker(cfg CheckerConfig) *DefaultChecker {
	return &DefaultChecker{cfg: cfg}
}

// Watch implements Checker. When EnableCheck is false it immediately emits HealthServing and
// closes the channel (R6.7). Otherwise it launches a goroutine that drives the Watch stream
// and falls back to periodic Check when the backend returns Unimplemented (R6.8).
func (c *DefaultChecker) Watch(ctx context.Context, conn *grpc.ClientConn) (<-chan HealthState, error) {
	ch := make(chan HealthState, 4)

	if !c.cfg.EnableCheck {
		// R6.7: health checking disabled — treat all subconnections as healthy immediately.
		go func() {
			ch <- HealthServing
			close(ch)
		}()
		return ch, nil
	}

	go c.runWatch(ctx, conn, ch)
	return ch, nil
}

// runWatch drives the streaming Health/Watch RPC. On Unimplemented it falls back to
// periodic Check RPCs. The goroutine exits when ctx is canceled, at which point ch is closed.
func (c *DefaultChecker) runWatch(ctx context.Context, conn *grpc.ClientConn, ch chan<- HealthState) {
	defer close(ch)

	client := healthpb.NewHealthClient(conn)
	req := &healthpb.HealthCheckRequest{Service: c.cfg.ServiceName}

	backoff := 1 * time.Second
	for {
		stream, err := client.Watch(ctx, req)
		if err != nil {
			if isUnimplemented(err) {
				// R6.8: Watch not supported — fall back to periodic Check.
				c.runPeriodicCheck(ctx, client, ch)
				return
			}
			// Transient dial error before stream was established; treat as not serving.
			if !sendState(ctx, ch, HealthNotServing) {
				return
			}
			// Wait before retrying
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				backoff *= 2
				if backoff > 10*time.Second {
					backoff = 10 * time.Second
				}
			}
			continue
		}

		// Reset backoff on successful watch establishment
		backoff = 1 * time.Second

		for {
			resp, err := stream.Recv()
			if err != nil {
				if ctx.Err() != nil {
					// Context canceled — normal shutdown.
					return
				}
				if isUnimplemented(err) {
					// R6.8: server stopped supporting Watch mid-stream; fall back.
					c.runPeriodicCheck(ctx, client, ch)
					return
				}
				// Other stream error; report not serving and break to retry Watch.
				if !sendState(ctx, ch, HealthNotServing) {
					return
				}
				break
			}

			if !sendState(ctx, ch, toHealthState(resp.Status)) {
				return
			}
		}
	}
}

// runPeriodicCheck issues Health/Check RPCs at cfg.Interval until ctx is canceled (R6.8).
// Each call is bounded by cfg.Timeout to satisfy R6.6.
func (c *DefaultChecker) runPeriodicCheck(ctx context.Context, client healthpb.HealthClient, ch chan<- HealthState) {
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()

	req := &healthpb.HealthCheckRequest{Service: c.cfg.ServiceName}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			state := c.doCheck(ctx, client, req)
			if !sendState(ctx, ch, state) {
				return
			}
		}
	}
}

// doCheck performs a single Health/Check RPC with the configured timeout (R6.6).
func (c *DefaultChecker) doCheck(ctx context.Context, client healthpb.HealthClient, req *healthpb.HealthCheckRequest) HealthState {
	checkCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	resp, err := client.Check(checkCtx, req)
	if err != nil {
		return HealthNotServing
	}
	return toHealthState(resp.Status)
}

// toHealthState maps a gRPC health proto status to a HealthState value.
func toHealthState(s healthpb.HealthCheckResponse_ServingStatus) HealthState {
	if s == healthpb.HealthCheckResponse_SERVING {
		return HealthServing
	}
	return HealthNotServing
}

// sendState tries to deliver state to ch, respecting ctx cancellation.
// Returns false if ctx is done and the send was not performed.
func sendState(ctx context.Context, ch chan<- HealthState, state HealthState) bool {
	select {
	case ch <- state:
		return true
	case <-ctx.Done():
		return false
	}
}

// isUnimplemented returns true when err carries a gRPC Unimplemented status code.
func isUnimplemented(err error) bool {
	return status.Code(err) == codes.Unimplemented
}
