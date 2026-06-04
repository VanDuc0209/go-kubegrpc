# Implementation Plan: grpc-k8s-balancer

## Overview

Convert the feature design into a series of prompts for a code-generation LLM that will implement each step with incremental progress. Make sure that each prompt builds on the previous prompts, and ends with wiring things together. There should be no hanging or orphaned code that isn't integrated into a previous step. Focus ONLY on tasks that involve writing, modifying, or testing code.

The implementation language is **Go** (per design). The library is built bottom-up: first the pure types (Target, Config), then the pluggable internals (Resolver, Pool, Health, Picker, Retry, Observability), and finally the public `Client` facade that wires a `*grpc.ClientConn` to the custom resolver and balancer builders.

In-scope per user-confirmed decisions:
- Target schemes: `k8s://` (authoritative, EndpointSlice watch) and `dns://` (fallback, periodic lookup) only.
- Built-in load-balancing policies: `round_robin` and `least_request` only. Custom policies are still registrable via the `Picker` factory registry (R5.7).

Property numbering used in this plan (derived from requirements; design's Correctness Properties section is intentionally minimal):
- **P1** Target round-trip: `ParseTarget(t.String(), t.Namespace) == t` for every valid `Target` (R2.5).
- **P2** Backoff formula: `attemptBackoff(n) == min(MaxBackoff, InitialBackoff * BackoffMultiplier^(n-1))` for all `n ≥ 1` (R7.4).
- **P3** Round-robin distribution: for `N` healthy SubConns and any starting counter, the next `N` picks form a permutation of those SubConns (R5.4).
- **P4** Reconcile is set difference: `toAdd = desired \ current` and `toRemove = current \ desired` for any pair of endpoint sets (R4.1, R4.5, R8.3, R8.4).
- **P5** Capacity cap: when `MaxSubconnections = M > 0`, the pool size after reconcile never exceeds `M`, and the kept set is the lexicographically smallest `M` of `desired` (R12.1, R12.2).
- **P6** Subconn identity uniqueness: the pool holds at most one SubConn per `(namespace, service, address, port)` tuple at all times (R4.5).
- **P7** Least-request determinism: when picking, the chosen SubConn has the minimum `InFlight` among healthy SubConns; ties are broken by lexicographic `address:port` (R5.5).

## Tasks

- [x] 1. Set up Go module, package layout, and shared scaffolding
  - Initialize `go.mod` with module path and Go 1.22+.
  - Create the directory tree from the design's "Package Layout" section (`internal/resolver/{k8s,dns}`, `internal/balancer`, `internal/health`, `internal/retry`, `internal/observability`, `internal/target`, `examples/`).
  - Add core dependencies: `google.golang.org/grpc`, `google.golang.org/grpc/health/grpc_health_v1`, `k8s.io/client-go`, `k8s.io/api`, `k8s.io/apimachinery`, `go.opentelemetry.io/otel/metric`, `log/slog` (stdlib).
  - Add a property-testing dependency: `pgregory.net/rapid` (selected for stdlib `testing.T` integration; PBT framework decision is local to tests).
  - Create an empty `errors.go` with `var Err... = errors.New(...)` placeholders for sentinel errors that later tasks will populate.
  - _Requirements: 1.7, 9.6_

- [x] 2. Implement Target parsing and pretty-printing
  - [x] 2.1 Implement the `Target`, `Port` types and `ParseTarget` / `Target.String`
    - Place public types in `target.go`; place internal scheme-aware tokenizers in `internal/target/parse.go`.
    - Accept `k8s:///<ns>/<svc>:<port>`, `k8s:///<svc>:<port>` (use `defaultNamespace` arg, R2.2), and `dns:///<host>:<port>`.
    - Distinguish numeric vs named port: numeric matches `^[0-9]+$` and parses to `Port{Number: n}`; otherwise `Port{Name: s}`.
    - Return typed errors for: unsupported scheme (R2.6), missing namespace with no default (R2.3), empty service/host, invalid port.
    - Implement `Target.String` so the output is parseable by `ParseTarget` with the same `defaultNamespace`.
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.6_

  - [x] 2.2 Write property test for Target round-trip
    - **Property P1: Target round-trip parse/print**
    - **Validates: Requirements 2.5**
    - Use `rapid` to generate valid Targets across both schemes, with random namespaces, service names, and either numeric (1..65535) or named ports.
    - Assert `ParseTarget(t.String(), t.Namespace) == t` and that re-parsing the printed form yields an identical struct.

  - [x] 2.3 Write unit tests for Target parsing error paths
    - Cover: scheme `static://` rejected (R2.6), missing namespace with empty `DefaultNamespace` rejected (R2.3), missing namespace with non-empty `DefaultNamespace` filled in (R2.2), invalid numeric port (`:0`, `:99999`), empty service name.
    - _Requirements: 2.2, 2.3, 2.6_

- [x] 3. Implement the public Config struct and defaults
  - [x] 3.1 Implement `Config`, `RetryPolicy`, `DefaultConfig`, and `(*Config).validate`
    - Place in `config.go`. Mirror field types from the design.
    - `validate` enforces: `LoadBalancingPolicy` ∈ {`round_robin`, `least_request`} or registered name; `MaxAttempts ≥ 1`; non-negative durations; `BackoffMultiplier ≥ 1.0`; `MaxSubconnections ≥ 0`; `RetryableStatusCodes` non-nil when `MaxAttempts > 1`.
    - Returned errors must name the offending field and the violated bound (R9.4).
    - `DefaultConfig` returns the values listed in the design ("DefaultConfig() returns" subsection).
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6_

  - [x] 3.2 Write unit tests for Config validation
    - Each invalid field produces an error whose message contains the field name.
    - `DefaultConfig()` passes `validate()`.
    - _Requirements: 9.2, 9.4_

- [x] 4. Implement the per-call `NonIdempotent` option and sentinel errors
  - In `option.go`, define `NonIdempotent() grpc.CallOption` using `grpc.EmptyCallOption` composed with a typed marker that the retry interceptor can detect via type assertion on `grpc.CallOption` slices.
  - In `errors.go`, add: `ErrTargetInvalid`, `ErrUnsupportedScheme`, `ErrMissingNamespace`, `ErrUnknownPolicy`, `ErrPolicyExists`, `ErrNoHealthySubconns`, `ErrClientClosed`.
  - Wrap these with `%w` at call sites in later tasks.
  - _Requirements: 5.3, 5.6, 5.7, 7.7_

- [x] 5. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Implement the internal Resolver interface and DNS fallback resolver
  - [x] 6.1 Define `Resolver`, `Snapshot`, `Endpoint`, `ResolverState` in `internal/resolver/resolver.go`
    - Match the signatures in the design's "Resolver Layer" section.
    - `Snapshot` carries the *full* set of endpoints; downstream reconcile derives deltas.
    - _Requirements: 3.1, 3.2, 3.3, 8.5_

  - [x] 6.2 Implement the DNS resolver in `internal/resolver/dns/`
    - `builder.go` registers a `google.golang.org/grpc/resolver.Builder` under scheme `dns`.
    - `lookup.go` runs a goroutine that calls `net.DefaultResolver.LookupIPAddr` (and `LookupSRV` if the Target port is named) every 30s; emits a `Snapshot` on each tick and on initial start.
    - On `LookupIPAddr` error, emit `Snapshot{Err: ..., State: ResolverDisconnected}` and continue.
    - Honor `ctx` cancellation to stop the loop and return from `Close`.
    - _Requirements: 3.1, 3.5_

  - [x] 6.3 Write unit tests for DNS resolver lookup loop
    - Inject a fake resolver function; verify the loop emits snapshots, handles errors without panicking, and stops on context cancel.
    - _Requirements: 3.5_

- [x] 7. Implement the Kubernetes resolver (`k8s://` scheme)
  - [x] 7.1 Implement Kubernetes client construction in `internal/resolver/k8s/kubeclient.go`
    - When `Config.Kubeconfig` is set, build with `clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)`; otherwise use `rest.InClusterConfig()`.
    - Return a typed error wrapping the underlying cause when neither path works.
    - _Requirements: 3.8, 3.9_

  - [x] 7.2 Implement the EndpointSlice watcher in `internal/resolver/k8s/watcher.go`
    - Use `informers.NewFilteredSharedInformerFactory` scoped to the Target namespace, filtering EndpointSlices by label `kubernetes.io/service-name=<svc>`.
    - On Add/Update/Delete, recompute the Endpoint set, filter by the Target's port (named or numeric, R3.7), exclude not-ready and terminating endpoints (R3.2, R3.3), and push a `Snapshot` to a buffered length-1 channel (drop-old semantics) consumed by the reconcile goroutine.
    - When the watch closes or errors, transition to `ResolverDisconnected` and start a backoff timer (R3.5): `1s, 2s, 4s, 8s, 16s, 30s, 30s,...`.
    - On reconnection, perform a full list and emit a single `Snapshot` reflecting the new state (R3.6).
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7_

  - [x] 7.3 Implement reconcile (set difference) in `internal/resolver/k8s/reconcile.go`
    - `func reconcile(current, desired map[Key]Endpoint) (toAdd, toRemove []Key)` where `Key = (address, port)`.
    - Used by both the k8s and dns paths; place under `internal/resolver/` if shared, or duplicate under each scheme — pick one home and import from there.
    - _Requirements: 4.1, 4.5, 8.3, 8.4_

  - [x] 7.4 Write property test for reconcile
    - **Property P4: Reconcile is set difference**
    - **Validates: Requirements 4.1, 4.5, 8.3, 8.4**
    - Generate random `current` and `desired` sets; assert `toAdd = desired \ current`, `toRemove = current \ desired`, and `toAdd ∩ toRemove = ∅`.

  - [x] 7.5 Implement the resolver state machine including the grace window
    - Track cumulative outage duration; when it exceeds `Config.ResolverGraceWindow`, transition to `ResolverInGraceWindow` (R8.5), keep the last-known endpoints, and log WARN every `ResolverGraceWindow` until reconnected.
    - On reconnect from any state, resume `ResolverConnected` and emit a fresh snapshot.
    - _Requirements: 8.1, 8.2, 8.5_

  - [x] 7.6 Write unit tests for the k8s resolver helpers
    - Port filter selects the right endpoint port for both numeric and named cases (R3.7).
    - Backoff sequence is monotone non-decreasing and capped at 30s (R3.5).
    - Grace window logs at the configured interval and not more often (use a fake clock).
    - _Requirements: 3.5, 3.7, 8.5_

- [x] 8. Implement the Subconnection pool
  - [x] 8.1 Implement `pool.go` in `internal/balancer/`
    - Pool keyed by `(namespace, service, address, port)` (R4.5); enforce uniqueness.
    - `Add(endpoint)` creates exactly one SubConn via the gRPC `balancer.ClientConn.NewSubConn` (R4.1).
    - `Remove(endpoint)` marks `Draining=true` (R4.3); start a 5s timer; close once in-flight count reaches zero or the timer fires (R4.2).
    - On a SubConn `connectivity.TransientFailure → permanent` signal, close and request resolver re-verification via a callback channel (R4.4).
    - When an endpoint reuses an existing `address:port` after a Pod restart, the previous SubConn is closed before the new one is created (R4.6).
    - Emit INFO log on every create/close with `endpoint`, `port`, and `reason` (R4.7).
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7_

  - [x] 8.2 Apply the `MaxSubconnections` cap deterministically
    - When `Config.MaxSubconnections > 0`, sort `desired` lexicographically by `address:port` and keep the first N (R12.2).
    - Endpoints beyond the cap are not added; if cap shrinks at runtime, the surplus is removed via the same drain path.
    - _Requirements: 12.1, 12.2_

  - [x] 8.3 Write property test for pool identity uniqueness
    - **Property P6: Subconn identity uniqueness**
    - **Validates: Requirements 4.5**
    - Drive the pool with random sequences of Add/Remove operations on a small key universe; assert at every step the pool contains at most one SubConn per key.

  - [x] 8.4 Write property test for the capacity cap
    - **Property P5: MaxSubconnections cap honored**
    - **Validates: Requirements 12.1, 12.2**
    - Generate random `desired` sets and caps `M`; assert pool size ≤ `M` and the kept set equals the lexicographically smallest `M`.

  - [x] 8.5 Write unit tests for drain timeout and restart eviction
    - In-flight RPCs delay close up to 5s (R4.2); timer expiry forces close.
    - Pod-restart eviction closes the old SubConn before a new one with the same key is created (R4.6).
    - _Requirements: 4.2, 4.6_

- [x] 9. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Implement the Health Checker
  - [x] 10.1 Implement `internal/health/checker.go`
    - For each SubConn, open a `grpc.health.v1.Health/Watch` stream using `Config.HealthCheckServiceName` (R6.1, R6.2).
    - On `codes.Unimplemented` from `Watch`, fall back to periodic `Check` at `Config.HealthCheckInterval` (R6.8).
    - Apply `Config.HealthCheckTimeout` to each probe attempt; timeout marks the SubConn unhealthy (R6.6).
    - When `Config.EnableHealthCheck = false`, emit a single `HealthServing` event and stop (R6.7).
    - _Requirements: 6.1, 6.2, 6.6, 6.7, 6.8_

  - [x] 10.2 Implement the health state machine in `internal/health/state.go`
    - Transitions: `Unknown → Serving | NotServing`, `Serving ↔ NotServing`. Each transition is published within 1s (R6.4, R6.5) by emitting on the event channel as soon as the probe response arrives.
    - Emit INFO log on every transition with the endpoint address and new state (R10.6).
    - _Requirements: 6.3, 6.4, 6.5, 10.6_

  - [x] 10.3 Write unit tests for health state transitions and Watch→Check fallback
    - Use an in-process gRPC server implementing `Health/Watch` and `Health/Check`; verify SERVING↔NOT_SERVING transitions are surfaced and the Unimplemented fallback works.
    - Verify `EnableHealthCheck=false` issues no probes.
    - _Requirements: 6.3, 6.4, 6.5, 6.7, 6.8_

- [x] 11. Implement the Balancer and Picker registry
  - [x] 11.1 Register a `balancer.Builder` named `k8s_balancer` in `internal/balancer/builder.go`
    - On `UpdateClientConnState`, recompute the desired endpoint set, drive the pool, and rebuild the active Picker via the configured `PickerFactory`.
    - Atomically swap the active Picker via `atomic.Pointer[balancer.Picker]` so the hot path is lock-free.
    - When zero healthy SubConns are present, install a "no-healthy" Picker that returns `status.Error(codes.Unavailable, "<target>: no healthy subconns")` (R5.6).
    - _Requirements: 5.6_

  - [x] 11.2 Implement `picker_rr.go` (round_robin)
    - Use an `atomic.Uint64` counter; `Pick` returns `healthy[counter.Add(1) % N]`.
    - `Done` is a no-op for round_robin.
    - _Requirements: 5.1, 5.4_

  - [x] 11.3 Implement `picker_lr.go` (least_request)
    - `Pick` scans `healthy` once, selects the SubConn with the smallest `InFlight` (atomic), tie-breaks by lexicographic `address:port` (R5.5).
    - `Pick` increments `InFlight` on the chosen SubConn; the returned `Done` callback decrements it.
    - _Requirements: 5.1, 5.5_

  - [x] 11.4 Implement the custom-policy registry in `internal/balancer/registry.go`
    - `RegisterPolicy(name string, factory PickerFactory) error`; pre-register `round_robin` and `least_request` in `init`.
    - Looking up an unknown policy returns `ErrUnknownPolicy` wrapped with the offending name and the list of registered names (R5.3).
    - Re-registering an existing name returns `ErrPolicyExists`.
    - Expose the registry through a public `RegisterPolicy` symbol in `balancer.go` (R5.7).
    - _Requirements: 5.1, 5.3, 5.7_

  - [x] 11.5 Write property test for round-robin distribution
    - **Property P3: Round-robin even distribution**
    - **Validates: Requirements 5.4**
    - For random `N ∈ [1, 32]` healthy SubConns and a random starting counter, run `N` `Pick` calls; assert the result multiset equals the SubConn set (each appears exactly once).

  - [x] 11.6 Write property test for least-request selection
    - **Property P7: Least-request picks min in-flight, deterministic tie-break**
    - **Validates: Requirements 5.5**
    - For random `InFlight` vectors, assert the picker selects an index whose `InFlight` equals the minimum, and on ties the one with the smallest `address:port` is chosen.

  - [x] 11.7 Write unit tests for the policy registry and error paths
    - Unknown policy from `Config.LoadBalancingPolicy` causes `NewClient` to return an error naming the value and listing supported policies (R5.3).
    - `RegisterPolicy` rejects duplicate names (R5.7).
    - Zero-healthy Picker returns `Unavailable` with the Target in the message (R5.6).
    - _Requirements: 5.3, 5.6, 5.7_

- [x] 12. Implement the Retry Coordinator
  - [x] 12.1 Implement `backoff.go` in `internal/retry/`
    - `attemptBackoff(n) = min(MaxBackoff, InitialBackoff * BackoffMultiplier^(n-1))` for `n ≥ 1` (R7.4).
    - Accept an injectable clock for testability.

  - [x] 12.2 Implement the unary retry interceptor in `internal/retry/interceptor.go`
    - On error with `status.Code(err) ∈ RetryableStatusCodes` and attempt < `MaxAttempts`, sleep `attemptBackoff(attempt)` then retry.
    - When at least two healthy SubConns exist, pass an `exclude` hint (or use a per-call key) so the Balancer Picks a different SubConn than the previous attempt (R7.3).
    - Stop and return the most recent error wrapped with `context.Cause(ctx)` if the context expires during the sleep or attempt (R7.5).
    - Skip retry when `NonIdempotent()` is present in the call options (R7.7) or `MaxAttempts ≤ 1` (R7.8).
    - Skip retry when the status code is not in `RetryableStatusCodes` (R7.6).
    - _Requirements: 7.1, 7.2, 7.3, 7.5, 7.6, 7.7, 7.8_

  - [x] 12.3 Implement the streaming retry interceptor
    - Retry is allowed only before the first message is sent on the wire; once `SendMsg` succeeds, retry is disabled for that stream.
    - Same status-code, attempt-count, and `NonIdempotent` rules as unary.
    - _Requirements: 7.2, 7.6, 7.7, 7.8_

  - [x] 12.4 Write property test for the backoff formula
    - **Property P2: Backoff = min(MaxBackoff, Initial * Mult^(n-1))**
    - **Validates: Requirements 7.4**
    - Generate random `(Initial, Mult ≥ 1.0, Max, n ∈ [1, 20])`; assert the function output matches the formula and is monotonically non-decreasing in `n` until clamped, then constant at `Max`.

  - [x] 12.5 Write unit tests for the retry decision tree
    - Cases: retryable code retries up to `MaxAttempts`; non-retryable returns immediately (R7.6); `NonIdempotent` disables retry (R7.7); `MaxAttempts=1` disables retry (R7.8); ctx cancel during backoff returns wrapped error (R7.5); two-healthy scenario picks a different SubConn on retry (R7.3).
    - _Requirements: 7.2, 7.3, 7.5, 7.6, 7.7, 7.8_

- [x] 13. Implement Observability (metrics + structured logs)
  - [x] 13.1 Implement `internal/observability/metrics.go`
    - Build OTel instruments via the meter `grpc-k8s-balancer` from `Config.MeterProvider` (or `otel.GetMeterProvider()` when nil) (R10.4).
    - Instruments: `rpc.started`, `rpc.completed`, `rpc.errors`, `subconn.created`, `subconn.closed`, `subconn.healthy`, `subconn.unhealthy`, `resolver.state`, `rpc.latency_ms` (R10.1, R10.2, R10.3) with the labels listed in the design.
    - Wire counters and histograms into the retry interceptor and balancer events.
    - _Requirements: 10.1, 10.2, 10.3, 10.4_

  - [x] 13.2 Implement `internal/observability/logging.go`
    - DEBUG log on every retry with `method`, `attempt`, `prior_code`, `endpoint` (R10.5).
    - INFO log on every health transition (R10.6) and on every SubConn create/close (R4.7) — sourced from health and pool components.
    - Honor `Config.Logger`; default to `slog.Default()` (R9.6).
    - _Requirements: 4.7, 9.6, 10.5, 10.6_

- [x] 14. Wire the public Client (`NewClient`, `Close`)
  - [x] 14.1 Implement `NewClient` in `balancer.go`
    - Validate `Config` (`DefaultConfig` if nil), parse Target with the configured `DefaultNamespace` (R2.2), and reject unknown `LoadBalancingPolicy` (R5.3).
    - Register the appropriate `resolver.Builder` (`k8s` or `dns`) and the `k8s_balancer` `balancer.Builder` once per process via `sync.Once` keyed by the Config-derived identity.
    - Build the `*grpc.ClientConn` via `grpc.NewClient(target.String(), append(internalDialOpts, cfg.DialOptions...)...)` where `internalDialOpts` injects: the unary+stream retry interceptors, the service-config selecting `k8s_balancer`, the credentials passthrough, and the user's logger via context (R9.5).
    - Return a `*client` whose underlying `*grpc.ClientConn` is exposed through `grpc.ClientConnInterface` (R1.1, R1.2).
    - On any error, return `(nil, err)` and ensure no goroutines or connections leak (R1.3).
    - _Requirements: 1.1, 1.2, 1.3, 5.3, 9.1, 9.5_

  - [x] 14.2 Implement `Close` with idempotency and goroutine bookkeeping
    - Track every spawned goroutine via a `sync.WaitGroup`.
    - `Close` cancels the root context, waits for the WaitGroup with a 5s deadline (R12.3), and closes the `*grpc.ClientConn` (R1.4, R1.5).
    - Guard with `sync.Once`; return `nil` on subsequent calls (R1.6).
    - _Requirements: 1.4, 1.5, 1.6, 12.3_

  - [x] 14.3 Implement `Invoke` and `NewStream` delegation
    - Forward to the underlying `*grpc.ClientConn`; the retry interceptors handle attempt selection, so the facade is a thin shim.
    - When the Client is closed, return `ErrClientClosed` instead of panicking on use-after-close.
    - _Requirements: 1.2, 11.1_

  - [x] 14.4 Write unit tests for the public API surface
    - Invalid Target string yields `(nil, error)` naming the invalid field (R1.3, R2.6).
    - Unknown `LoadBalancingPolicy` returns the error described in R5.3.
    - `Close` is idempotent and returns nil on the second call (R1.6); double-Close does not panic.
    - The returned Client satisfies `grpc.ClientConnInterface` (compile-time assertion `var _ grpc.ClientConnInterface = (*client)(nil)`).
    - _Requirements: 1.2, 1.3, 1.6, 5.3_

- [x] 15. End-to-end concurrency and recovery integration tests
  - [x] 15.1 Write a race-detector integration test for concurrent RPCs and resolver churn
    - Stand up an in-process gRPC test server cluster (multiple `bufconn` listeners simulated as endpoints) and a fake informer driven by hand-crafted EndpointSlice events.
    - Run a goroutine pool issuing concurrent unary and streaming RPCs while the fake informer adds and removes endpoints; pass under `go test -race`.
    - Assert no errors surface to callers when a SubConn is closed while a Pick races against it (R11.3).
    - _Requirements: 11.1, 11.2, 11.3_

  - [x] 15.2 Write a recovery integration test for scale-up, scale-down, and total outage
    - Drive the fake informer through: scale to 0 endpoints (zero healthy), then back to N endpoints; verify RPCs succeed again without reconstructing the Client (R8.1, R8.2).
    - Scale up by K endpoints; assert exactly K new SubConns appear within 1s (R8.3).
    - Scale down by K endpoints; assert exactly K SubConns are closed and the closed set matches the removed endpoints (R8.4).
    - Simulate a Kubernetes API outage longer than `ResolverGraceWindow`; assert the last-known endpoints remain in use and a WARN log is emitted at the configured interval (R8.5).
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_

- [x] 16. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP. They cover unit tests, property-based tests, and integration tests.
- Each task references specific sub-requirement clauses (not just user stories) for traceability.
- Property-based tests use `pgregory.net/rapid` and are placed close to the unit they validate so failures localize quickly.
- Checkpoints (tasks 5, 9, 16) ensure incremental validation across the three natural seams: pure types → discovery+pool → balancer+retry+wiring.
- The implementation is purely additive against `google.golang.org/grpc` — no fork, no monkey-patching — by registering custom `resolver.Builder` and `balancer.Builder` instances under unique names.

---

This workflow (requirements → design → tasks) is now complete. The artifacts in `.kiro/specs/grpc-k8s-balancer/` are ready. To begin implementation, open `tasks.md` and click "Start task" next to any task item.
