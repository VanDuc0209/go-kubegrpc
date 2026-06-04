package balancer

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	grpcbalancer "google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	grpcresolver "google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"

	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/health"
	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/resolver"
)

// BalancerName is the name under which the k8s balancer is registered with
// gRPC's global balancer registry. Callers select it via a service-config or
// grpc.WithDefaultServiceConfig.
const BalancerName = "k8s_balancer"

type configKey struct{}

// ConfigKey is the attribute key used to pass the balancer configuration
// from the resolver to the balancer builder.
var ConfigKey = configKey{}

// AttemptTracker tracks the addresses of subconnections tried during a single
// RPC call sequence (including retries) to enable failover exclusions (Task 4).
type AttemptTracker struct {
	Mu    sync.Mutex
	Addrs []string
}

type trackerContextKey struct{}

// TrackerKey is the context key used to store the AttemptTracker.
var TrackerKey = trackerContextKey{}

// BuildConfig carries the runtime configuration the builder needs when
// constructing a k8sBalancer. NewClient populates activeConfig before the
// first gRPC dial so that Build() can read it.
type BuildConfig struct {
	Namespace              string
	Service                string
	Policy                 string
	MaxSubconns            int
	Logger                 *slog.Logger
	EnableHealthCheck      bool
	HealthCheckServiceName string
	HealthCheckInterval    time.Duration
	HealthCheckTimeout     time.Duration
}


// activeConfig is the package-level configuration set by NewClient before
// registering the balancer. Access is protected by activeConfigMu.
var (
	activeConfigMu sync.RWMutex
	activeConfig   *BuildConfig
)

// SetActiveConfig sets the package-level BuildConfig used by the next Build
// call. It is called by NewClient before the gRPC dial.
func SetActiveConfig(cfg *BuildConfig) {
	activeConfigMu.Lock()
	defer activeConfigMu.Unlock()
	activeConfig = cfg
}

// getActiveConfig returns a snapshot of the current BuildConfig under the
// read lock. Returns a zero-value BuildConfig when none has been set.
func getActiveConfig() BuildConfig {
	activeConfigMu.RLock()
	defer activeConfigMu.RUnlock()
	if activeConfig == nil {
		return BuildConfig{}
	}
	return *activeConfig
}

// init registers the k8s_balancer with gRPC so that any ClientConn whose
// service-config names "k8s_balancer" will use this implementation.
func init() {
	grpcbalancer.Register(&k8sBalancerBuilder{})
}

// k8sBalancerBuilder implements grpcbalancer.Builder.
type k8sBalancerBuilder struct{}

func (b *k8sBalancerBuilder) Name() string { return BalancerName }

// Build creates a new k8sBalancer, capturing the current BuildConfig snapshot.
func (b *k8sBalancerBuilder) Build(cc grpcbalancer.ClientConn, _ grpcbalancer.BuildOptions) grpcbalancer.Balancer {
	return newK8sBalancer(cc, getActiveConfig())
}

// ---------------------------------------------------------------------------
// k8sBalancer
// ---------------------------------------------------------------------------

// k8sBalancer is the gRPC balancer implementation for k8s_balancer.
//
// It owns a Pool of SubConns and rebuilds an immutable Picker whenever the
// healthy set changes. The active Picker is stored behind an atomic pointer so
// that the hot RPC pick path is lock-free.
type k8sBalancer struct {
	cc     grpcbalancer.ClientConn
	cfg    BuildConfig
	pool   *Pool
	logger *slog.Logger

	mu     sync.Mutex
	policy string // resolved policy name, set once in Build

	// picker holds the current immutable Picker. It is swapped atomically
	// whenever the healthy SubConn set changes so Pick never contends with
	// reconciliation.
	picker atomicPickerWrapper
}

// atomicPickerWrapper wraps grpcbalancer.Picker behind a sync/atomic value to
// allow lock-free loads on the hot path. We store a *pickerBox so that the
// atomic value is always a concrete pointer type.
type atomicPickerWrapper struct {
	v sync.Map // key: struct{}{}, value: grpcbalancer.Picker
}

func (a *atomicPickerWrapper) Store(p grpcbalancer.Picker) {
	a.v.Store(struct{}{}, p)
}

func (a *atomicPickerWrapper) Load() grpcbalancer.Picker {
	v, ok := a.v.Load(struct{}{})
	if !ok {
		return nil
	}
	return v.(grpcbalancer.Picker)
}

// newK8sBalancer constructs a k8sBalancer and installs the initial no-op picker.
func newK8sBalancer(cc grpcbalancer.ClientConn, cfg BuildConfig) *k8sBalancer {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	policy := cfg.Policy
	if policy == "" {
		policy = "round_robin"
	}

	b := &k8sBalancer{
		cc:     cc,
		cfg:    cfg,
		logger: logger,
		policy: policy,
	}

	b.pool = NewPool(cc, logger, func(key PoolKey) {
		// R4.4: on TransientFailure, ask gRPC to re-resolve. ResolveNow is a
		// best-effort hint; the empty options struct signals no special behaviour.
		cc.ResolveNow(grpcresolver.ResolveNowOptions{})
	})

	// Install a no-healthy Picker as the initial state until the first
	// UpdateClientConnState arrives (R5.6).
	b.installNoHealthyPicker()

	return b
}



// ---------------------------------------------------------------------------
// grpcbalancer.Balancer implementation
// ---------------------------------------------------------------------------

// UpdateClientConnState is called by gRPC whenever the set of resolved
// addresses changes. It reconciles the SubConn pool and rebuilds the Picker.
//
// Addresses are extracted from state.ResolverState.Addresses; each address
// carries an Addr string in "host:port" form which is parsed back to an
// Endpoint for pool reconciliation.
func (b *k8sBalancer) UpdateClientConnState(state grpcbalancer.ClientConnState) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Extract configuration from resolver attributes if present (Task 2).
	if state.ResolverState.Attributes != nil {
		if val := state.ResolverState.Attributes.Value(ConfigKey); val != nil {
			if bCfg, ok := val.(*BuildConfig); ok {
				b.cfg = *bCfg
				if bCfg.Policy != "" {
					b.policy = bCfg.Policy
				}
				if bCfg.Logger != nil {
					b.logger = bCfg.Logger
				}
			}
		}
	}


	// Convert gRPC addresses to resolver.Endpoints.
	desired := make(map[resolver.Key]resolver.Endpoint, len(state.ResolverState.Addresses))
	for _, addr := range state.ResolverState.Addresses {
		ep, err := parseAddrToEndpoint(addr.Addr)
		if err != nil {
			b.logger.Warn("balancer: skipping unparseable address",
				slog.String("addr", addr.Addr),
				slog.String("error", err.Error()),
			)
			continue
		}
		desired[resolver.KeyOf(ep)] = ep
	}

	// Apply MaxSubconnections cap to the desired endpoint set.
	desiredSlice := make([]resolver.Endpoint, 0, len(desired))
	for _, ep := range desired {
		desiredSlice = append(desiredSlice, ep)
	}
	desiredSlice = CapEndpoints(desiredSlice, b.cfg.MaxSubconns)

	// Re-build desired map from the capped slice.
	cappedDesired := make(map[resolver.Key]resolver.Endpoint, len(desiredSlice))
	for _, ep := range desiredSlice {
		cappedDesired[resolver.KeyOf(ep)] = ep
	}

	// Compute current set from the pool snapshot.
	current := make(map[resolver.Key]resolver.Endpoint)
	for _, entry := range b.pool.Snapshot() {
		ep := resolver.Endpoint{
			Address: entry.Key.Address,
			Port:    entry.Key.Port,
			Ready:   true,
		}
		current[resolver.KeyOf(ep)] = ep
	}

	// Reconcile: add new endpoints, remove stale ones.
	for key, ep := range cappedDesired {
		if _, exists := current[key]; !exists {
			if _, err := b.pool.Add(b.cfg.Namespace, b.cfg.Service, ep); err != nil {
				b.logger.Error("balancer: failed to add subconn",
					slog.String("endpoint", fmt.Sprintf("%s:%d", ep.Address, ep.Port)),
					slog.String("error", err.Error()),
				)
			}
		}
	}
	for key, ep := range current {
		if _, exists := cappedDesired[key]; !exists {
			poolKey := PoolKey{
				Namespace: b.cfg.Namespace,
				Service:   b.cfg.Service,
				Address:   ep.Address,
				Port:      ep.Port,
			}
			b.pool.Remove(poolKey)
		}
	}

	// Rebuild the picker from the updated healthy set.
	b.rebuildPickerLocked()

	return nil
}

// UpdateSubConnState is called by gRPC when a SubConn's connectivity state
// changes. It updates the pool's connectivity tracking and, when the SubConn
// transitions to Ready, marks it as healthy so it becomes eligible for picks.
func (b *k8sBalancer) UpdateSubConnState(sc grpcbalancer.SubConn, state grpcbalancer.SubConnState) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Find the pool entry that owns this SubConn.
	key, found := b.findKeyForSubConn(sc)
	if !found {
		return
	}

	b.pool.UpdateConnState(key, state.ConnectivityState)

	switch state.ConnectivityState {
	case connectivity.Ready:
		if b.cfg.EnableHealthCheck {
			// Find entry and start health check (Task 3).
			if entry, ok := b.getPoolEntry(key); ok && entry.HealthCancel == nil {
				b.startHealthCheck(entry)
			}
		} else {
			b.pool.UpdateHealth(key, HealthServing)
		}
	case connectivity.TransientFailure, connectivity.Shutdown:
		if entry, ok := b.getPoolEntry(key); ok {
			b.stopHealthCheck(entry)
		}
		b.pool.UpdateHealth(key, HealthNotServing)
	case connectivity.Idle:
		sc.Connect()
	default:
		// Idle / Connecting: leave health state unchanged until resolved.
	}

	b.rebuildPickerLocked()
}

// startHealthCheck launches active health check probing on entry (Task 3).
func (b *k8sBalancer) startHealthCheck(entry *SubConnEntry) {
	hCfg := health.CheckerConfig{
		ServiceName: b.cfg.HealthCheckServiceName,
		Interval:    b.cfg.HealthCheckInterval,
		Timeout:     b.cfg.HealthCheckTimeout,
		EnableCheck: b.cfg.EnableHealthCheck,
	}
	checker := health.NewChecker(hCfg)
	addr := entry.Key.addrPort()

	hConn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.logger.Warn("balancer: failed to dial health check connection",
			slog.String("endpoint", addr),
			slog.String("error", err.Error()),
		)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	entry.HealthCancel = cancel

	ch, err := checker.Watch(ctx, hConn)
	if err != nil {
		b.logger.Warn("balancer: failed to start health check watch",
			slog.String("endpoint", addr),
			slog.String("error", err.Error()),
		)
		cancel()
		_ = hConn.Close()
		entry.HealthCancel = nil
		return
	}

	sm := health.NewStateMachine(addr, func(state health.HealthState) {
		b.mu.Lock()
		defer b.mu.Unlock()

		poolState := HealthUnknown
		switch state {
		case health.HealthServing:
			poolState = HealthServing
		case health.HealthNotServing:
			poolState = HealthNotServing
		}

		b.pool.UpdateHealth(entry.Key, poolState)
		b.rebuildPickerLocked()
	}, b.cfg.Logger)

	go func() {
		sm.Drain(ctx, ch)
		_ = hConn.Close()
	}()
}

// stopHealthCheck stops the active health check on entry (Task 3).
func (b *k8sBalancer) stopHealthCheck(entry *SubConnEntry) {
	if entry.HealthCancel != nil {
		entry.HealthCancel()
		entry.HealthCancel = nil
	}
}

// getPoolEntry retrieves a SubConnEntry by key from the pool snapshot.
func (b *k8sBalancer) getPoolEntry(key PoolKey) (*SubConnEntry, bool) {
	for _, entry := range b.pool.Snapshot() {
		if entry.Key == key {
			return entry, true
		}
	}
	return nil, false
}


// Close is called when the ClientConn is torn down. It logs the event; the
// pool lifecycle is managed externally.
func (b *k8sBalancer) Close() {
	b.logger.Info("balancer: closing", slog.String("policy", b.policy))
}

// ResolverError is called by gRPC when the resolver encounters an error.
func (b *k8sBalancer) ResolverError(err error) {
	b.logger.Warn("balancer: resolver error", slog.String("error", err.Error()))
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// rebuildPickerLocked constructs a new Picker from the current healthy entries
// and atomically installs it. Caller must hold b.mu.
func (b *k8sBalancer) rebuildPickerLocked() {
	healthy := b.pool.HealthyEntries()

	// Sort for determinism across calls.
	sort.Slice(healthy, func(i, j int) bool {
		ki := fmt.Sprintf("%s:%d", healthy[i].Key.Address, healthy[i].Key.Port)
		kj := fmt.Sprintf("%s:%d", healthy[j].Key.Address, healthy[j].Key.Port)
		return ki < kj
	})

	var newPicker grpcbalancer.Picker
	if len(healthy) == 0 {
		// R5.6: no healthy SubConns → install a Picker that always returns Unavailable.
		newPicker = &noHealthyPicker{target: b.cfg.Service}
	} else {
		factory, err := LookupPolicy(b.policy)
		if err != nil {
			// Fall back to round-robin if the configured policy is missing.
			b.logger.Warn("balancer: unknown policy, falling back to round_robin",
				slog.String("policy", b.policy),
				slog.String("error", err.Error()),
			)
			factory = newRRPicker
		}
		newPicker = factory(healthy)
	}

	b.picker.Store(newPicker)

	// Notify gRPC of the new Picker so it can start/continue routing.
	b.cc.UpdateState(grpcbalancer.State{
		ConnectivityState: connectivityStateForHealthy(len(healthy)),
		Picker:            newPicker,
	})
}

// installNoHealthyPicker installs the initial no-healthy Picker without
// acquiring b.mu (called from newK8sBalancer before the balancer is exposed).
func (b *k8sBalancer) installNoHealthyPicker() {
	p := &noHealthyPicker{target: b.cfg.Service}
	b.picker.Store(p)
}

// findKeyForSubConn scans the pool snapshot for an entry whose SubConn matches
// sc and returns its PoolKey. Returns (zero, false) when not found.
func (b *k8sBalancer) findKeyForSubConn(sc grpcbalancer.SubConn) (PoolKey, bool) {
	for _, entry := range b.pool.Snapshot() {
		if entry.SubConn == sc {
			return entry.Key, true
		}
	}
	return PoolKey{}, false
}

// connectivityStateForHealthy maps a healthy count to a gRPC connectivity
// state for the UpdateState call.
func connectivityStateForHealthy(n int) connectivity.State {
	if n > 0 {
		return connectivity.Ready
	}
	return connectivity.TransientFailure
}

// parseAddrToEndpoint parses an "host:port" string into a resolver.Endpoint.
func parseAddrToEndpoint(addr string) (resolver.Endpoint, error) {
	var host string
	var port int
	_, err := fmt.Sscanf(addr, "%s", &addr) // normalise whitespace
	if err != nil {
		return resolver.Endpoint{}, fmt.Errorf("balancer: empty address")
	}

	// Find last colon to split host and port (handles IPv6 addresses).
	lastColon := -1
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			lastColon = i
			break
		}
	}
	if lastColon < 0 {
		return resolver.Endpoint{}, fmt.Errorf("balancer: address %q has no port", addr)
	}

	host = addr[:lastColon]
	portStr := addr[lastColon+1:]
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		return resolver.Endpoint{}, fmt.Errorf("balancer: address %q has non-numeric port %q", addr, portStr)
	}
	if port <= 0 || port > 65535 {
		return resolver.Endpoint{}, fmt.Errorf("balancer: address %q has out-of-range port %d", addr, port)
	}

	return resolver.Endpoint{
		Address: host,
		Port:    port,
		Ready:   true,
	}, nil
}

// ---------------------------------------------------------------------------
// noHealthyPicker
// ---------------------------------------------------------------------------

// noHealthyPicker is installed when the healthy SubConn set is empty. Every
// Pick call returns codes.Unavailable with the target in the message (R5.6).
type noHealthyPicker struct {
	target string
}

func (p *noHealthyPicker) Pick(_ grpcbalancer.PickInfo) (grpcbalancer.PickResult, error) {
	msg := "no healthy subconns"
	if p.target != "" {
		msg = fmt.Sprintf("%s: no healthy subconns", p.target)
	}
	return grpcbalancer.PickResult{}, status.Error(codes.Unavailable, msg)
}
