# Requirements Document

## Introduction

This feature defines a Go library that wraps the official gRPC-Go client to provide native client-side load balancing, dynamic service discovery, and connection lifecycle management for services running in Kubernetes. The library acts as a middleware layer between application code and the underlying gRPC client, allowing Go services to discover backend pods of a Kubernetes Service in real time, maintain a healthy pool of subconnections, balance RPCs across them, and recover automatically from pod restarts and scaling events. The goal is to reduce reliance on service mesh sidecars (Istio, Linkerd) for east-west gRPC traffic by providing equivalent capabilities directly inside the application process.

The library is consumed as a Go module. Its public API SHALL be the only interaction surface for application developers; all Kubernetes API access, watch loops, connection pooling, retry, and health-check logic SHALL be encapsulated behind that API.

## Glossary

- **Library**: The Go module produced by this feature, wrapping `google.golang.org/grpc` and providing Kubernetes-aware client capabilities.
- **Client**: The public type exposed by the Library that application code uses to invoke gRPC methods (e.g., a wrapper around `*grpc.ClientConn` or a balancer-aware `grpc.ClientConnInterface`).
- **Resolver**: The component inside the Library that translates a logical target (a Kubernetes Service reference) into a list of backend Endpoints by querying the Kubernetes API.
- **Balancer**: The component inside the Library that selects one Subconnection per RPC according to a configured load-balancing policy.
- **Subconnection**: A single gRPC connection (`*grpc.ClientConn` or equivalent transport) maintained by the Library to one backend Endpoint.
- **Endpoint**: A `host:port` pair corresponding to a ready Pod backing a Kubernetes Service.
- **Kubernetes Service**: A native Kubernetes `Service` object identified by `namespace/name` whose `EndpointSlice` (or `Endpoints`) objects enumerate the backing Pods.
- **EndpointSlice**: The Kubernetes `discovery.k8s.io/v1` resource the Library watches to learn about backend Endpoints.
- **Pod**: A Kubernetes Pod backing the Service. A Pod is `Ready` when its readiness conditions are satisfied and `Terminating` when its `deletionTimestamp` is set.
- **Health_Checker**: The component inside the Library that probes Subconnections using the gRPC Health Checking Protocol (`grpc.health.v1.Health`).
- **Retry_Policy**: The configurable policy describing which RPC errors are retried, the maximum attempts, and backoff timing.
- **Failover**: Re-issuing a failed RPC against a different Endpoint when the original Endpoint is unhealthy or unreachable.
- **Target**: A Library-specific URI or struct identifying a Kubernetes Service (e.g., `k8s:///namespace/service-name:port-name`).

## Requirements

### Requirement 1: Library Public API and Client Lifecycle

**User Story:** As a Go service developer, I want to construct a Client by pointing at a Kubernetes Service, so that I can invoke gRPC methods without managing connections manually.

#### Acceptance Criteria

1. THE Library SHALL expose a constructor that accepts a Target, an optional configuration struct, and a `context.Context`, and returns a Client and an error.
2. WHEN the constructor is called with a syntactically valid Target, THE Library SHALL return a non-nil Client that implements `grpc.ClientConnInterface`.
3. IF the Target is syntactically invalid, THEN THE Library SHALL return a nil Client and a non-nil error that names the invalid field.
4. THE Client SHALL expose a `Close` method that releases all Subconnections, stops the Resolver, and stops the Health_Checker.
5. WHEN `Close` is called, THE Library SHALL return only after all background goroutines started by the Client have exited.
6. IF `Close` is called more than once on the same Client, THEN THE Library SHALL return nil on subsequent calls without panicking.
7. THE Library SHALL NOT require the application to import `k8s.io/client-go` directly to use the Client.

### Requirement 2: Target Parsing and Pretty-Printing

**User Story:** As a Go service developer, I want a single string Target to identify a Kubernetes Service and port, so that configuration is concise and reviewable.

#### Acceptance Criteria

1. THE Target_Parser SHALL accept strings of the form `k8s:///<namespace>/<service-name>:<port>` where `<port>` is either a numeric port or a named port from the Service spec.
2. WHEN a Target string omits the namespace segment (`k8s:///<service-name>:<port>`), THE Target_Parser SHALL resolve the namespace from the configuration field `DefaultNamespace`.
3. IF the configuration field `DefaultNamespace` is empty AND the Target string omits the namespace, THEN THE Target_Parser SHALL return an error identifying the missing namespace.
4. THE Target_Printer SHALL format a parsed Target struct back into a string.
5. FOR ALL valid Target strings, parsing the string and then printing the resulting struct SHALL produce a string that, when parsed again, yields a Target struct equal to the first parsed struct (round-trip property).
6. IF the Target string uses any scheme other than `k8s`, THEN THE Target_Parser SHALL return an error naming the unsupported scheme.

### Requirement 3: Kubernetes Service Discovery via EndpointSlice Watch

**User Story:** As a Go service developer, I want the Library to discover backend pods automatically, so that my service stays connected as pods come and go.

#### Acceptance Criteria

1. THE Resolver SHALL list and watch `discovery.k8s.io/v1/EndpointSlice` objects whose `kubernetes.io/service-name` label matches the Target's service name in the Target's namespace.
2. WHEN the Resolver receives an EndpointSlice event indicating a new Ready Endpoint, THE Library SHALL add the Endpoint to the Subconnection pool within 1 second of receiving the event.
3. WHEN the Resolver receives an EndpointSlice event indicating an Endpoint transitioned to not Ready or Terminating, THE Library SHALL remove the Endpoint from the load-balancing rotation within 1 second of receiving the event.
4. WHILE the Kubernetes API watch connection is active, THE Resolver SHALL NOT poll the Kubernetes API on a fixed interval for the same Service.
5. IF the Kubernetes API watch connection is closed or returns an error, THEN THE Resolver SHALL re-establish the watch using exponential backoff starting at 1 second and capped at 30 seconds.
6. WHEN the Resolver re-establishes the watch after a disconnection, THE Resolver SHALL reconcile the current EndpointSlice state with the existing Subconnection pool by adding missing Endpoints and removing stale Endpoints.
7. THE Resolver SHALL filter Endpoints by the Target's port, selecting only the port whose name or number matches the Target.
8. THE Library SHALL authenticate to the Kubernetes API using in-cluster service account credentials when running inside a Pod.
9. WHERE the configuration field `Kubeconfig` is set, THE Library SHALL authenticate to the Kubernetes API using the specified kubeconfig file instead of in-cluster credentials.

### Requirement 4: Subconnection Lifecycle Management

**User Story:** As a Go service developer, I want the Library to manage one connection per backend pod, so that I do not leak connections or send traffic to dead pods.

#### Acceptance Criteria

1. WHEN a new Endpoint is added by the Resolver, THE Library SHALL create exactly one Subconnection to that Endpoint.
2. WHEN an Endpoint is removed by the Resolver, THE Library SHALL close the corresponding Subconnection within 5 seconds, after draining in-flight RPCs assigned to that Subconnection.
3. WHILE a Subconnection is being drained, THE Balancer SHALL NOT route new RPCs to that Subconnection.
4. IF a Subconnection's transport reports a permanent failure, THEN THE Library SHALL close the Subconnection and request the Resolver to verify the Endpoint still exists.
5. THE Library SHALL maintain at most one Subconnection per `(namespace, service-name, endpoint-address, port)` tuple at any time.
6. WHEN a Pod is restarted and reuses the same Endpoint address, THE Library SHALL close the previous Subconnection and create a new Subconnection rather than reusing the old transport.
7. THE Library SHALL emit a structured log entry at INFO level whenever a Subconnection is created or closed, containing the Endpoint address, port, and reason.

### Requirement 5: Client-Side Load Balancing

**User Story:** As a Go service developer, I want the Library to distribute RPCs across all healthy backends, so that load is balanced without a sidecar.

#### Acceptance Criteria

1. THE Library SHALL provide at least two built-in load-balancing policies: `round_robin` and `least_request`.
2. THE configuration struct SHALL accept a `LoadBalancingPolicy` field whose value selects the active policy.
3. IF the `LoadBalancingPolicy` field is set to a value not recognized by the Library, THEN THE Library SHALL return an error from the Client constructor naming the unrecognized value and listing the supported values.
4. WHEN the active policy is `round_robin` AND there are N healthy Subconnections, THE Balancer SHALL distribute the next N RPCs across the N Subconnections such that each Subconnection receives exactly one RPC.
5. WHEN the active policy is `least_request`, THE Balancer SHALL select the healthy Subconnection with the lowest count of in-flight RPCs at the time of selection.
6. IF there are zero healthy Subconnections at the time of an RPC, THEN THE Library SHALL fail the RPC with a gRPC status code of `Unavailable` and a message identifying the Target.
7. THE Library SHALL expose an interface that allows applications to register a custom load-balancing policy by name.

### Requirement 6: Health Checking

**User Story:** As a Go service developer, I want the Library to detect unhealthy backends, so that traffic only goes to working pods.

#### Acceptance Criteria

1. THE Health_Checker SHALL probe each Subconnection using the gRPC Health Checking Protocol (`grpc.health.v1.Health/Check` or `Watch`).
2. THE configuration struct SHALL accept a `HealthCheckServiceName` field whose value is sent as the `service` argument of the health probe.
3. WHILE a Subconnection's last health probe response was `SERVING`, THE Balancer SHALL consider the Subconnection healthy.
4. WHEN a Subconnection's health probe response transitions from `SERVING` to any other value, THE Balancer SHALL mark the Subconnection unhealthy within 1 second.
5. WHEN a Subconnection's health probe response transitions to `SERVING` after being unhealthy, THE Balancer SHALL mark the Subconnection healthy within 1 second.
6. IF the Health_Checker cannot reach the Subconnection for the duration configured in `HealthCheckTimeout`, THEN THE Balancer SHALL mark the Subconnection unhealthy.
7. WHERE the configuration field `EnableHealthCheck` is `false`, THE Library SHALL treat all Subconnections as healthy and SHALL NOT issue health probes.
8. THE Health_Checker SHALL use the streaming `Watch` RPC when supported by the backend and SHALL fall back to periodic `Check` RPCs at the interval configured in `HealthCheckInterval` when `Watch` returns `Unimplemented`.

### Requirement 7: Retry and Failover

**User Story:** As a Go service developer, I want the Library to retry transient failures on different backends, so that single-pod failures do not surface as RPC errors.

#### Acceptance Criteria

1. THE configuration struct SHALL accept a Retry_Policy with fields `MaxAttempts`, `InitialBackoff`, `MaxBackoff`, `BackoffMultiplier`, and `RetryableStatusCodes`.
2. WHEN an RPC fails with a status code listed in `RetryableStatusCodes`, THE Library SHALL retry the RPC up to `MaxAttempts` total attempts.
3. WHEN the Library retries an RPC, THE Library SHALL select a different healthy Subconnection than the one used by the previous attempt, when at least two healthy Subconnections exist.
4. THE Library SHALL apply exponential backoff between retry attempts using `InitialBackoff * BackoffMultiplier^(attempt-1)`, clamped at `MaxBackoff`.
5. IF the RPC's `context.Context` deadline expires during a retry attempt or backoff, THEN THE Library SHALL stop retrying and return the most recent error wrapped with the deadline reason.
6. THE Library SHALL NOT retry RPCs whose status code is not listed in `RetryableStatusCodes`.
7. THE Library SHALL NOT retry RPCs flagged as non-idempotent by the caller via a per-call option exposed by the Library.
8. WHERE `MaxAttempts` is `1`, THE Library SHALL behave as if retry is disabled.

### Requirement 8: Recovery on Pod Restart and Scaling

**User Story:** As a Go service developer, I want the Library to recover automatically when pods restart or the Service scales, so that I do not need to restart my application.

#### Acceptance Criteria

1. WHEN all backend Pods of the Service have been deleted, THE Library SHALL keep the Client open and continue watching for new Endpoints.
2. WHEN at least one new Ready Endpoint appears after a period of zero healthy Subconnections, THE Library SHALL resume serving RPCs without requiring the Client to be reconstructed.
3. WHEN the Service scales up by K Pods, THE Library SHALL open up to K additional Subconnections within 1 second of the corresponding EndpointSlice update.
4. WHEN the Service scales down by K Pods, THE Library SHALL close exactly K Subconnections, selecting the Subconnections corresponding to the Endpoints removed from the EndpointSlice.
5. IF the Kubernetes API server is unreachable for longer than the duration configured in `ResolverGraceWindow`, THEN THE Library SHALL keep using the last-known set of Endpoints and SHALL log a WARN entry every `ResolverGraceWindow` interval until connectivity is restored.

### Requirement 9: Configuration Surface

**User Story:** As a Go service developer, I want a single configuration struct, so that I can tune the Library without learning its internals.

#### Acceptance Criteria

1. THE Library SHALL expose a configuration struct whose fields cover at minimum: `DefaultNamespace`, `Kubeconfig`, `LoadBalancingPolicy`, `EnableHealthCheck`, `HealthCheckServiceName`, `HealthCheckInterval`, `HealthCheckTimeout`, `RetryPolicy`, `ResolverGraceWindow`, `DialOptions`, and `Logger`.
2. WHEN a configuration field is omitted, THE Library SHALL apply a documented default value for that field.
3. THE Library SHALL expose a function that returns the default configuration struct.
4. IF a configuration field has a value outside its documented valid range, THEN THE Library SHALL return an error from the Client constructor naming the field and the violated bound.
5. THE configuration struct SHALL accept a slice of `grpc.DialOption` values via the `DialOptions` field, and the Library SHALL apply those options to every Subconnection.
6. WHERE the `Logger` field is set, THE Library SHALL emit all log entries through the supplied logger; otherwise THE Library SHALL emit log entries through the standard library `log/slog` default logger.

### Requirement 10: Observability

**User Story:** As a Go service developer, I want metrics and logs from the Library, so that I can diagnose connection and balancing issues in production.

#### Acceptance Criteria

1. THE Library SHALL expose counters for: total RPCs started, total RPCs completed, total RPC errors grouped by status code, total Subconnection creations, and total Subconnection closures.
2. THE Library SHALL expose gauges for: current healthy Subconnection count, current unhealthy Subconnection count, and current resolver state (connected or disconnected).
3. THE Library SHALL expose a histogram for RPC latency in milliseconds, labeled by gRPC method and status code.
4. THE Library SHALL emit metrics via the OpenTelemetry metrics API using a meter named `grpc-k8s-balancer`.
5. WHEN a retry occurs, THE Library SHALL emit a structured log entry at DEBUG level containing the gRPC method, attempt number, prior status code, and selected Endpoint.
6. WHEN a Subconnection transitions between healthy and unhealthy, THE Library SHALL emit a structured log entry at INFO level containing the Endpoint address and the new state.

### Requirement 11: Concurrency and Safety

**User Story:** As a Go service developer, I want the Client to be safe for concurrent use, so that my service can issue RPCs from many goroutines.

#### Acceptance Criteria

1. THE Client SHALL be safe for concurrent use by multiple goroutines for all methods of `grpc.ClientConnInterface`.
2. WHEN the Resolver updates the Endpoint set concurrently with in-flight RPCs, THE Library SHALL NOT cause data races as detected by `go test -race`.
3. IF an RPC is selected to use a Subconnection that is concurrently being closed, THEN THE Library SHALL retry the selection on the next healthy Subconnection without surfacing the close as an error to the caller.

### Requirement 12: Resource Bounds

**User Story:** As a Go service developer, I want predictable resource usage, so that the Library does not exhaust file descriptors or memory.

#### Acceptance Criteria

1. THE configuration struct SHALL accept a `MaxSubconnections` field; when set to a positive integer N, THE Library SHALL maintain at most N Subconnections per Client.
2. WHEN the number of Ready Endpoints exceeds `MaxSubconnections`, THE Library SHALL select the first `MaxSubconnections` Endpoints in deterministic lexicographic order of `address:port` and SHALL NOT open Subconnections to the remainder.
3. THE Library SHALL release all goroutines, watches, timers, and Subconnections associated with a Client within 5 seconds of `Close` returning.
