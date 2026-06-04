package balancer

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	grpcbalancer "google.golang.org/grpc/balancer"
)

// Local sentinel errors for the registry. These mirror the public sentinels in
// the root package but are defined here to avoid an import cycle between the
// root package and internal/balancer.
var (
	// ErrPolicyExists is returned by RegisterPolicy when the given name has
	// already been registered.
	ErrPolicyExists = errors.New("grpc-k8s-balancer: policy already registered")

	// ErrUnknownPolicy is returned by LookupPolicy when the given name has not
	// been registered.
	ErrUnknownPolicy = errors.New("grpc-k8s-balancer: unknown load balancing policy")
)

// PickerFactory builds a grpcbalancer.Picker from the current set of healthy
// SubConnEntries. The factory is called each time the healthy set changes so
// that Pickers remain immutable once constructed.
type PickerFactory func(healthy []*SubConnEntry) grpcbalancer.Picker

// registryMu guards the registry map for concurrent access.
var (
	registryMu sync.RWMutex
	registry   = make(map[string]PickerFactory)
)

// init pre-registers the two built-in load-balancing policies so that callers
// can reference "round_robin" and "least_request" without an explicit
// RegisterPolicy call.
func init() {
	registry["round_robin"] = newRRPicker
	registry["least_request"] = newLRPicker
}

// RegisterPolicy adds a custom load-balancing policy under name.
// Returns ErrPolicyExists (wrapped with the offending name) if name is already
// taken, satisfying R5.7.
func RegisterPolicy(name string, factory PickerFactory) error {
	registryMu.Lock()
	defer registryMu.Unlock()

	if _, exists := registry[name]; exists {
		return fmt.Errorf("policy %q already registered: %w", name, ErrPolicyExists)
	}
	registry[name] = factory
	return nil
}

// LookupPolicy retrieves the PickerFactory registered under name.
// Returns ErrUnknownPolicy (wrapped with the offending name and the list of
// known policies) when name has not been registered, satisfying R5.3.
func LookupPolicy(name string) (PickerFactory, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	factory, ok := registry[name]
	if !ok {
		known := knownPoliciesLocked()
		return nil, fmt.Errorf("policy %q not registered (known: %v): %w", name, known, ErrUnknownPolicy)
	}
	return factory, nil
}

// KnownPolicies returns the sorted list of registered policy names.
func KnownPolicies() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return knownPoliciesLocked()
}

// knownPoliciesLocked returns the sorted policy names; caller must hold at
// least a read lock on registryMu.
func knownPoliciesLocked() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
