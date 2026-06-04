package resolver

import (
	"testing"

	"pgregory.net/rapid"
)

// Small key universe: 5 possible addresses × 3 possible ports = at most 15 unique keys.
var (
	possibleAddresses = []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5"}
	possiblePorts     = []int{8080, 9090, 50051}
)

// genKey draws a Key from the constrained universe.
func genKey(t *rapid.T, label string) Key {
	addr := rapid.SampledFrom(possibleAddresses).Draw(t, label+"_addr")
	port := rapid.SampledFrom(possiblePorts).Draw(t, label+"_port")
	return Key{Address: addr, Port: port}
}

// genEndpointMap builds a map[Key]Endpoint by drawing a slice of keys from the
// small universe. Duplicates collapse naturally when inserted into the map,
// which is the correct behaviour for a set represented as a map.
func genEndpointMap(t *rapid.T, label string) map[Key]Endpoint {
	keys := rapid.SliceOfN(
		rapid.Custom(func(rt *rapid.T) Key { return genKey(rt, label) }),
		0, 15,
	).Draw(t, label+"_keys")

	m := make(map[Key]Endpoint, len(keys))
	for _, k := range keys {
		m[k] = Endpoint{Address: k.Address, Port: k.Port, Ready: true}
	}
	return m
}

// toSet converts a []Key slice into a map[Key]struct{} for O(1) membership
// checks inside property assertions.
func toSet(keys []Key) map[Key]struct{} {
	s := make(map[Key]struct{}, len(keys))
	for _, k := range keys {
		s[k] = struct{}{}
	}
	return s
}

// TestReconcileSetDifference verifies Property P4:
//   - toAdd  = desired \ current  (keys in desired but NOT in current)
//   - toRemove = current \ desired  (keys in current but NOT in desired)
//   - toAdd ∩ toRemove = ∅
//   - every key in desired \ current appears in toAdd     (completeness)
//   - every key in current \ desired appears in toRemove  (completeness)
//
// Validates: Requirements 4.1, 4.5, 8.3, 8.4
func TestReconcileSetDifference(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		current := genEndpointMap(rt, "current")
		desired := genEndpointMap(rt, "desired")

		toAdd, toRemove := Reconcile(current, desired)

		addSet := toSet(toAdd)
		removeSet := toSet(toRemove)

		// 1. Every key in toAdd must be in desired but NOT in current.
		for _, k := range toAdd {
			if _, inDesired := desired[k]; !inDesired {
				rt.Fatalf("toAdd key %+v is not present in desired", k)
			}
			if _, inCurrent := current[k]; inCurrent {
				rt.Fatalf("toAdd key %+v is already present in current", k)
			}
		}

		// 2. Every key in toRemove must be in current but NOT in desired.
		for _, k := range toRemove {
			if _, inCurrent := current[k]; !inCurrent {
				rt.Fatalf("toRemove key %+v is not present in current", k)
			}
			if _, inDesired := desired[k]; inDesired {
				rt.Fatalf("toRemove key %+v is still present in desired", k)
			}
		}

		// 3. toAdd ∩ toRemove = ∅  (disjointness)
		for _, k := range toAdd {
			if _, inRemove := removeSet[k]; inRemove {
				rt.Fatalf("key %+v appears in both toAdd and toRemove", k)
			}
		}

		// 4. Completeness: every key in desired \ current must appear in toAdd.
		for k := range desired {
			if _, inCurrent := current[k]; !inCurrent {
				if _, inAdd := addSet[k]; !inAdd {
					rt.Fatalf("key %+v is in desired\\current but missing from toAdd", k)
				}
			}
		}

		// 5. Completeness: every key in current \ desired must appear in toRemove.
		for k := range current {
			if _, inDesired := desired[k]; !inDesired {
				if _, inRemove := removeSet[k]; !inRemove {
					rt.Fatalf("key %+v is in current\\desired but missing from toRemove", k)
				}
			}
		}
	})
}

// TestReconcileIdentityIdempotent verifies that reconciling a map with itself
// always produces empty toAdd and toRemove, for any input X:
//
//	Reconcile(X, X) → ([], [])
//
// Validates: Requirements 4.1, 4.5
func TestReconcileIdentityIdempotent(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		x := genEndpointMap(rt, "x")

		toAdd, toRemove := Reconcile(x, x)

		if len(toAdd) != 0 {
			rt.Fatalf("Reconcile(X, X) returned non-empty toAdd: %v", toAdd)
		}
		if len(toRemove) != 0 {
			rt.Fatalf("Reconcile(X, X) returned non-empty toRemove: %v", toRemove)
		}
	})
}
