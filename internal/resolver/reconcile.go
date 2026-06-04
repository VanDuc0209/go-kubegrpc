package resolver

// Reconcile computes the set difference between current and desired endpoint sets.
//
// toAdd = desired \ current  (endpoints in desired but not in current)
// toRemove = current \ desired  (endpoints in current but not in desired)
//
// Both current and desired are maps from Key to Endpoint.
// The returned slices contain only the Keys; the caller retrieves the full
// Endpoints from the respective maps.
//
// Properties guaranteed (P4):
//   - toAdd contains exactly the keys present in desired but absent in current.
//   - toRemove contains exactly the keys present in current but absent in desired.
//   - toAdd and toRemove are disjoint: toAdd ∩ toRemove = ∅.
//
// Requirements: R4.1, R4.5, R8.3, R8.4.
func Reconcile(current, desired map[Key]Endpoint) (toAdd, toRemove []Key) {
	for k := range desired {
		if _, exists := current[k]; !exists {
			toAdd = append(toAdd, k)
		}
	}

	for k := range current {
		if _, exists := desired[k]; !exists {
			toRemove = append(toRemove, k)
		}
	}

	return toAdd, toRemove
}
