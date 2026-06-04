package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "grpc-k8s-balancer"

// Metrics holds all OTel instruments for the library.
type Metrics struct {
	RPCStarted       metric.Int64Counter
	RPCCompleted     metric.Int64Counter
	RPCErrors        metric.Int64Counter
	SubConnCreated   metric.Int64Counter
	SubConnClosed    metric.Int64Counter
	SubConnHealthy   metric.Int64UpDownCounter
	SubConnUnhealthy metric.Int64UpDownCounter
	ResolverState    metric.Int64UpDownCounter
	RPCLatencyMs     metric.Float64Histogram
}

// NewMetrics creates all instruments using the provided MeterProvider.
// If provider is nil, uses otel.GetMeterProvider() (R10.4).
func NewMetrics(provider metric.MeterProvider) (*Metrics, error) {
	if provider == nil {
		provider = otel.GetMeterProvider()
	}

	meter := provider.Meter(meterName)

	rpcStarted, err := meter.Int64Counter(
		"rpc.started",
		metric.WithDescription("Number of RPCs started, labeled by method."),
	)
	if err != nil {
		return nil, err
	}

	rpcCompleted, err := meter.Int64Counter(
		"rpc.completed",
		metric.WithDescription("Number of RPCs completed, labeled by method and code."),
	)
	if err != nil {
		return nil, err
	}

	rpcErrors, err := meter.Int64Counter(
		"rpc.errors",
		metric.WithDescription("Number of RPC errors, labeled by method and code."),
	)
	if err != nil {
		return nil, err
	}

	subConnCreated, err := meter.Int64Counter(
		"subconn.created",
		metric.WithDescription("Number of SubConns created, labeled by endpoint."),
	)
	if err != nil {
		return nil, err
	}

	subConnClosed, err := meter.Int64Counter(
		"subconn.closed",
		metric.WithDescription("Number of SubConns closed, labeled by endpoint and reason."),
	)
	if err != nil {
		return nil, err
	}

	subConnHealthy, err := meter.Int64UpDownCounter(
		"subconn.healthy",
		metric.WithDescription("Current number of healthy SubConns."),
	)
	if err != nil {
		return nil, err
	}

	subConnUnhealthy, err := meter.Int64UpDownCounter(
		"subconn.unhealthy",
		metric.WithDescription("Current number of unhealthy SubConns."),
	)
	if err != nil {
		return nil, err
	}

	resolverState, err := meter.Int64UpDownCounter(
		"resolver.state",
		metric.WithDescription("Current resolver state, labeled by state."),
	)
	if err != nil {
		return nil, err
	}

	rpcLatencyMs, err := meter.Float64Histogram(
		"rpc.latency_ms",
		metric.WithDescription("RPC latency in milliseconds, labeled by method and code."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	return &Metrics{
		RPCStarted:       rpcStarted,
		RPCCompleted:     rpcCompleted,
		RPCErrors:        rpcErrors,
		SubConnCreated:   subConnCreated,
		SubConnClosed:    subConnClosed,
		SubConnHealthy:   subConnHealthy,
		SubConnUnhealthy: subConnUnhealthy,
		ResolverState:    resolverState,
		RPCLatencyMs:     rpcLatencyMs,
	}, nil
}

// RecordRPCStart increments the rpc.started counter.
func (m *Metrics) RecordRPCStart(ctx context.Context, method string) {
	m.RPCStarted.Add(ctx, 1,
		metric.WithAttributes(attribute.String("method", method)),
	)
}

// RecordRPCComplete increments rpc.completed and records latency.
func (m *Metrics) RecordRPCComplete(ctx context.Context, method, code string, latencyMs float64) {
	attrs := metric.WithAttributes(
		attribute.String("method", method),
		attribute.String("code", code),
	)
	m.RPCCompleted.Add(ctx, 1, attrs)
	m.RPCLatencyMs.Record(ctx, latencyMs, attrs)
}

// RecordRPCError increments rpc.errors.
func (m *Metrics) RecordRPCError(ctx context.Context, method, code string) {
	m.RPCErrors.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("method", method),
			attribute.String("code", code),
		),
	)
}

// RecordSubConnCreated increments subconn.created.
func (m *Metrics) RecordSubConnCreated(ctx context.Context, endpoint string) {
	m.SubConnCreated.Add(ctx, 1,
		metric.WithAttributes(attribute.String("endpoint", endpoint)),
	)
}

// RecordSubConnClosed increments subconn.closed.
func (m *Metrics) RecordSubConnClosed(ctx context.Context, endpoint, reason string) {
	m.SubConnClosed.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("endpoint", endpoint),
			attribute.String("reason", reason),
		),
	)
}

// UpdateHealthCount adjusts the subconn.healthy and subconn.unhealthy gauges.
// If healthy is true, delta is applied to subconn.healthy; otherwise to subconn.unhealthy.
func (m *Metrics) UpdateHealthCount(ctx context.Context, delta int64, healthy bool) {
	if healthy {
		m.SubConnHealthy.Add(ctx, delta)
	} else {
		m.SubConnUnhealthy.Add(ctx, delta)
	}
}

// UpdateResolverState adjusts the resolver.state gauge for the given state label.
func (m *Metrics) UpdateResolverState(ctx context.Context, state string, delta int64) {
	m.ResolverState.Add(ctx, delta,
		metric.WithAttributes(attribute.String("state", state)),
	)
}
