//go:build tools

// Package grpck8sbalancer tools anchors build-time and test-time dependencies
// that are not yet imported by production code so that go mod tidy retains them.
package grpck8sbalancer

import (
	// gRPC and health proto – core transport dependency (R1.7).
	_ "google.golang.org/grpc"
	_ "google.golang.org/grpc/health/grpc_health_v1"

	// Kubernetes client-go, API, and apimachinery – used by k8s resolver (R3.8, R3.9).
	_ "k8s.io/api/core/v1"
	_ "k8s.io/apimachinery/pkg/apis/meta/v1"
	_ "k8s.io/client-go/kubernetes"

	// OpenTelemetry metrics – observability layer (R10.4).
	_ "go.opentelemetry.io/otel/metric"

	// Property-based testing framework – used in test packages.
	_ "pgregory.net/rapid"
)
