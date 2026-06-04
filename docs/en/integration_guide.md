# Integration Guide: `go-kubegrpc`

This guide explains how to install, configure, and integrate the `go-kubegrpc` library into your Go applications for Kubernetes-native client-side gRPC load balancing.

---

## 1. Installation

Add the library to your Go module dependencies:

```bash
go get github.com/grpc-k8s-balancer/grpc-k8s-balancer
```

Ensure your Go version is `1.22.0` or higher.

---

## 2. Kubernetes RBAC Setup

Because the library lists and watches `EndpointSlice` resources in real time to discover pod IP addresses, the Kubernetes ServiceAccount running your application must have RBAC permissions to read `endpointslices`.

Apply the following ClusterRole/Role and RoleBinding to your application namespace:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  namespace: <your-app-namespace>
  name: go-kubegrpc-resolver
rules:
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  namespace: <your-app-namespace>
  name: go-kubegrpc-resolver-binding
subjects:
  - kind: ServiceAccount
    name: default # Replace with your application's ServiceAccount name
    namespace: <your-app-namespace>
roleRef:
  kind: Role
  name: go-kubegrpc-resolver
  apiGroup: rbac.authorization.k8s.io
```

---

## 3. Target Formatting

The library parses targets to determine the scheme and logical endpoints:

1. **Kubernetes Target (Authoritative):**
   * Format: `k8s:///<namespace>/<service-name>:<port>`
   * Omitted Namespace: `k8s:///<service-name>:<port>` (uses `DefaultNamespace` from `Config`).
   * Port can be **numeric** (e.g. `8080`) or **named** (e.g. `grpc` matching the Service spec port name).
   * Example: `k8s:///prod/billing-service:grpc`

2. **DNS Fallback Target:**
   * Format: `dns:///<host>:<port>` (used for local development or out-of-cluster endpoints).
   * Example: `dns:///localhost:50051`

---

## 4. Quick Start Example

Here is a minimal example demonstrating how to initialize and use the client:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	grpck8s "github.com/grpc-k8s-balancer/grpc-k8s-balancer"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Get default configs and override defaults as needed
	cfg := grpck8s.DefaultConfig()
	cfg.DefaultNamespace = "billing"
	cfg.LoadBalancingPolicy = "least_request" // "round_robin" or "least_request"
	cfg.EnableHealthCheck = true

	// Construct the client
	// It automatically watches EndpointSlice resources in background
	client, err := grpck8s.NewClient(ctx, "k8s:///billing/payment-service:grpc", cfg)
	if err != nil {
		log.Fatalf("Failed to initialize client: %v", err)
	}
	defer client.Close() // Release resources, stop watchers, and close connections

	// Invoke gRPC RPCs
	// The balancer distributes traffic across healthy pods automatically
	var reply emptypb.Empty
	err = client.Invoke(ctx, "/PaymentService/ProcessPayment", &emptypb.Empty{}, &reply)
	if err != nil {
		log.Printf("RPC Error: %v", err)
	} else {
		fmt.Println("Payment processed successfully!")
	}
}
```

---

## 5. Configuration Options

You can customize the client's behavior by overriding fields on the `Config` struct returned by `DefaultConfig()`:

| Field | Type | Default | Description |
|---|---|---|---|
| `DefaultNamespace` | `string` | `""` | Namespace used when the target string omits it. |
| `Kubeconfig` | `string` | `""` | Path to kubeconfig (local dev). Uses in-cluster service account when empty. |
| `LoadBalancingPolicy`| `string` | `"round_robin"`| Balance policy: `"round_robin"` or `"least_request"`. |
| `EnableHealthCheck` | `bool` | `true` | Enables active gRPC health probing (`grpc.health.v1`). |
| `HealthCheckServiceName`| `string`| `""` | The service name argument sent inside health probes. |
| `HealthCheckInterval`| `time.Duration`| `10s` | Period between Check probes if streaming Watch is unimplemented. |
| `HealthCheckTimeout` | `time.Duration`| `2s` | Timeout for each health check probe. |
| `ResolverGraceWindow`| `time.Duration`| `30s` | Period last-known endpoints are served during API server outages. |
| `MaxSubconnections` | `int` | `0` (unbounded) | Maximum subconnections maintained per client. |
| `DialOptions` | `[]grpc.DialOption`| `nil` | Extra dial options (e.g., credentials, trace interceptors). |

---

## 6. Advanced Customizations

### 6.1 Per-Call Gating (Non-Idempotent Calls)
By default, failed requests with status codes listed in `RetryPolicy.RetryableStatusCodes` are automatically retried on a different backend. You can disable retries for a specific call by passing the `NonIdempotent()` call option:

```go
client.Invoke(ctx, method, in, out, grpck8sbalancer.NonIdempotent())
```

### 6.2 Custom Load Balancing Policy Registration
You can register a custom load balancing policy picker factory:

```go
import (
	grpck8s "github.com/grpc-k8s-balancer/grpc-k8s-balancer"
	internal "github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/balancer"
)

func init() {
	grpck8s.RegisterPolicy("custom_random", func(healthy []*internal.SubConnEntry) grpcbalancer.Picker {
		return &myRandomPicker{healthy: healthy}
	})
}
```

---

## 7. Best Practices

1. **Context Cancellation on Shutdown:** Always cancel the parent `context.Context` passed to `NewClient` or call `client.Close()` when shutting down your application. This ensures Kubernetes Informers and health checks stop immediately and connections drain gracefully (up to a 5-second deadline) without leaking goroutines.
2. **Configuring TLS:** If your backends require TLS/mTLS, pass the transport credentials in the `DialOptions` field of your `Config` struct. They will take precedence over the default insecure credentials.
3. **Graceful Disconnection (Draining):** When a backend pod terminates, the resolver notifies the client. The subconnection to that pod is moved to a draining state, blocking new requests while allowing in-flight requests up to 5 seconds to complete. Do not kill backend pods abruptly; let gRPC drain them.
