package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/resolver"
)

// reconnectBackoff defines the exponential backoff sequence used when the
// Kubernetes API watch connection is lost (R3.5). After the last element the
// delay stays capped at 30 s.
var reconnectBackoff = []time.Duration{
	1 * time.Second,
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
	16 * time.Second,
	30 * time.Second,
}

// Port identifies a Service port either by name or by number. Exactly one
// field is non-zero for a valid Port.
type Port struct {
	// Name is the named port from the Service spec (e.g. "grpc").
	// Set only when the Target port is not numeric.
	Name string

	// Number is the numeric port (1–65535).
	// Set only when the Target port is numeric.
	Number int
}

// WatcherConfig is the configuration for the k8s EndpointSlice watcher.
type WatcherConfig struct {
	// Namespace is the Kubernetes namespace that contains the Service.
	Namespace string

	// ServiceName is the Kubernetes Service name whose EndpointSlices are watched.
	ServiceName string

	// Port is the target port, either named or numeric (R3.7).
	Port Port

	// KubeClient is the Kubernetes client used to watch EndpointSlices.
	KubeClient kubernetes.Interface

	// Logger is the structured logger. When nil, slog.Default() is used.
	Logger *slog.Logger
}

// Watcher watches EndpointSlices for a specific Kubernetes Service and
// implements resolver.Resolver. It uses a SharedInformerFactory scoped to the
// target namespace, filtered by the service-name label (R3.1).
type Watcher struct {
	cfg       WatcherConfig
	closeOnce sync.Once
	cancel    context.CancelFunc
	done      chan struct{}
}

// NewWatcher creates a new Watcher. Call Start to begin watching.
func NewWatcher(cfg WatcherConfig) *Watcher {
	return &Watcher{
		cfg:  cfg,
		done: make(chan struct{}),
	}
}

// Start implements resolver.Resolver.Start. It launches a background goroutine
// that drives an informer factory and calls fn with each new Snapshot. Start
// returns immediately; it does not block.
func (w *Watcher) Start(ctx context.Context, fn func(resolver.Snapshot)) error {
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	go func() {
		defer close(w.done)
		w.run(ctx, fn)
	}()

	return nil
}

// Close implements resolver.Resolver.Close. It is idempotent; subsequent calls
// are no-ops and return nil (R1.6).
func (w *Watcher) Close() error {
	w.closeOnce.Do(func() {
		if w.cancel != nil {
			w.cancel()
		}
	})
	// Wait for the background goroutine to finish so all resources are released.
	<-w.done
	return nil
}

// run is the outer retry loop. It calls watch in a loop, backing off between
// attempts according to reconnectBackoff (R3.5).
func (w *Watcher) run(ctx context.Context, fn func(resolver.Snapshot)) {
	backoffIdx := 0
	for {
		err := w.watch(ctx, fn)
		if ctx.Err() != nil {
			// Context was canceled (Close called or parent ctx done); exit cleanly.
			return
		}
		// watch ended with an error — surface the disconnect and back off.
		fn(resolver.Snapshot{
			State: resolver.ResolverDisconnected,
			Err:   err,
		})
		sleep := reconnectBackoff[min(backoffIdx, len(reconnectBackoff)-1)]
		backoffIdx++
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return
		}
	}
}

// watch creates a filtered SharedInformerFactory for the target namespace and
// service, waits for the cache to sync, registers Add/Update/Delete event
// handlers, and blocks until ctx is canceled. It returns nil on clean shutdown
// or an error when the informer factory fails to sync within the deadline.
func (w *Watcher) watch(ctx context.Context, fn func(resolver.Snapshot)) error {
	logger := w.cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// Build the label selector so the informer only receives EndpointSlices
	// that belong to the target Service (R3.1).
	labelSelector := labels.SelectorFromSet(labels.Set{
		"kubernetes.io/service-name": w.cfg.ServiceName,
	})

	// Create a factory scoped to the target namespace. The tweakListOptions
	// callback injects our label selector on every List/Watch call so the API
	// server performs server-side filtering (R3.4 — no polling).
	factory := informers.NewFilteredSharedInformerFactory(
		w.cfg.KubeClient,
		0, // no resync period — rely purely on watch events (R3.4)
		w.cfg.Namespace,
		func(opts *metav1.ListOptions) {
			opts.LabelSelector = labelSelector.String()
		},
	)

	informer := factory.Discovery().V1().EndpointSlices().Informer()

	// sendSnapshot recomputes the full endpoint set from all currently-known
	// EndpointSlices held in the informer's cache, filters by target port, and
	// invokes fn with a fresh Connected Snapshot (R3.6).
	sendSnapshot := func() {
		rawList := informer.GetStore().List()
		slices := make([]discoveryv1.EndpointSlice, 0, len(rawList))
		for _, obj := range rawList {
			if es, ok := obj.(*discoveryv1.EndpointSlice); ok {
				slices = append(slices, *es)
			}
		}
		endpoints := extractEndpoints(slices, w.cfg.Port)
		fn(resolver.Snapshot{
			Endpoints: endpoints,
			State:     resolver.ResolverConnected,
		})
	}

	// Register event handlers. Each handler recomputes the full endpoint set
	// from the informer's cache so we always emit a coherent Snapshot rather
	// than trying to derive deltas from individual events (R3.6).
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(_ any) {
			sendSnapshot()
		},
		UpdateFunc: func(_, _ any) {
			sendSnapshot()
		},
		DeleteFunc: func(_ any) {
			sendSnapshot()
		},
	})
	if err != nil {
		return fmt.Errorf("adding EndpointSlice event handler: %w", err)
	}

	// Start the informer factory goroutines; they stop when stopCh is closed.
	stopCh := make(chan struct{})
	factory.Start(stopCh)

	// WaitForCacheSync blocks until the initial list has been processed and the
	// local cache is consistent with the API server state (R3.6 — full list on
	// (re)connect before emitting a Snapshot).
	syncCtx, syncCancel := context.WithTimeout(ctx, 30*time.Second)
	defer syncCancel()

	synced := cache.WaitForCacheSync(syncCtx.Done(), informer.HasSynced)
	if !synced {
		// Close the informer goroutines before returning the error.
		close(stopCh)
		if ctx.Err() != nil {
			// Parent context canceled — not a real error.
			return nil
		}
		return fmt.Errorf(
			"timed out waiting for EndpointSlice cache sync (namespace=%s, service=%s)",
			w.cfg.Namespace, w.cfg.ServiceName,
		)
	}

	logger.Info("k8s resolver: cache synced",
		slog.String("namespace", w.cfg.Namespace),
		slog.String("service", w.cfg.ServiceName),
	)

	// Emit the initial Snapshot after the cache is warm (R3.6).
	sendSnapshot()

	// Block until the context is canceled (Close called or parent done).
	<-ctx.Done()

	// Signal the informer factory to stop its goroutines.
	close(stopCh)
	return nil
}

// extractEndpoints builds the flat []resolver.Endpoint slice from a set of
// EndpointSlices. It applies two filters:
//
//  1. Port filter (R3.7): only the port matching targetPort (by name or number)
//     is considered. EndpointSlices without a matching port are skipped.
//
//  2. Readiness filter (R3.2, R3.3): endpoints whose Conditions.Ready is
//     false or whose Conditions.Terminating is true are excluded.
func extractEndpoints(slices []discoveryv1.EndpointSlice, targetPort Port) []resolver.Endpoint {
	var result []resolver.Endpoint

	for _, slice := range slices {
		// Step 1 — find the port in this EndpointSlice that matches targetPort.
		resolvedPort, found := matchPort(slice.Ports, targetPort)
		if !found {
			continue
		}

		// Step 2 — iterate over the endpoints in this slice.
		for _, ep := range slice.Endpoints {
			// Skip not-ready endpoints (R3.2).
			if ep.Conditions.Ready != nil && !*ep.Conditions.Ready {
				continue
			}
			// Skip terminating endpoints (R3.3).
			if ep.Conditions.Terminating != nil && *ep.Conditions.Terminating {
				continue
			}

			// Add one resolver.Endpoint per address in the endpoint.
			for _, addr := range ep.Addresses {
				result = append(result, resolver.Endpoint{
					Address: addr,
					Port:    resolvedPort,
					Ready:   true,
				})
			}
		}
	}

	return result
}

// matchPort finds the numeric port value from an EndpointSlice's port list
// that matches the given targetPort (by name or number). It returns the
// resolved port number and true when a match is found, otherwise 0 and false.
func matchPort(ports []discoveryv1.EndpointPort, targetPort Port) (int, bool) {
	for _, p := range ports {
		if p.Port == nil {
			continue
		}
		switch {
		case targetPort.Name != "":
			// Named port match (R3.7).
			if p.Name != nil && *p.Name == targetPort.Name {
				return int(*p.Port), true
			}
		case targetPort.Number != 0:
			// Numeric port match (R3.7).
			if int(*p.Port) == targetPort.Number {
				return int(*p.Port), true
			}
		}
	}
	return 0, false
}

// min returns the smaller of a and b. Provided for Go versions < 1.21 that
// don't have the built-in min; harmless in 1.22+ since the built-in takes
// precedence for typed int arguments only from untyped context.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
