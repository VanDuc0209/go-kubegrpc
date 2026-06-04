package balancer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"pgregory.net/rapid"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// makeEntries creates n SubConnEntries with fake SubConns and distinct addresses.
// All entries are marked HealthServing so they are eligible for picks.
func makeEntries(n int) []*SubConnEntry {
	entries := make([]*SubConnEntry, n)
	for i := range entries {
		entries[i] = &SubConnEntry{
			Key: PoolKey{Address: fmt.Sprintf("10.0.0.%d", i+1), Port: 8080},
			SubConn: &fakeSubConn{id: i, addr: fmt.Sprintf("10.0.0.%d:8080", i+1)},
		}
		entries[i].Health = HealthServing
	}
	return entries
}

// ---------------------------------------------------------------------------
// Task 11.5 – P3: Round-robin even distribution
// Validates: Requirements 5.4
// ---------------------------------------------------------------------------

// TestRRPickerDistribution verifies Property P3:
//
//	For N healthy SubConns, N consecutive picks starting from a fresh picker
//	form a permutation (each SubConn is selected exactly once).
//
// The rrPicker counter starts at 0. Add(1) % N cycles through indices
// 1%N, 2%N, …, N%N (=0), covering all indices 0..N-1 exactly once.
//
// Validates: Requirements 5.4
func TestRRPickerDistribution(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate a random N in [1, 32].
		n := rapid.IntRange(1, 32).Draw(rt, "n")

		entries := makeEntries(n)
		picker := newRRPicker(entries)

		// Collect the SubConns returned by N consecutive Pick calls.
		picked := make(map[balancer.SubConn]int, n)
		for range n {
			result, err := picker.Pick(balancer.PickInfo{})
			if err != nil {
				rt.Fatalf("Pick returned unexpected error: %v", err)
			}
			picked[result.SubConn]++
		}

		// Each of the N input SubConns must appear exactly once.
		if len(picked) != n {
			rt.Fatalf("expected %d distinct SubConns, got %d", n, len(picked))
		}
		for i, entry := range entries {
			count, ok := picked[entry.SubConn]
			if !ok {
				rt.Fatalf("entry[%d] (%s) was never picked in %d rounds", i, entry.Key.addrPort(), n)
			}
			if count != 1 {
				rt.Fatalf("entry[%d] (%s) was picked %d times, expected exactly 1", i, entry.Key.addrPort(), count)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Task 11.6 – P7: Least-request picks min in-flight, deterministic tie-break
// Validates: Requirements 5.5
// ---------------------------------------------------------------------------

// TestLRPickerMinInFlight verifies Property P7:
//
//  1. The chosen SubConn has InFlight == min(all InFlight values).
//  2. On ties, the entry with the lexicographically smallest "address:port" is chosen.
//  3. After Pick, the chosen entry's InFlight is incremented by 1.
//
// Validates: Requirements 5.5
func TestLRPickerMinInFlight(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate a random slice of in-flight counts, length 1-10.
		inFlightVec := rapid.SliceOfN(
			rapid.Int64Range(0, 20),
			1, 10,
		).Draw(rt, "in_flight")

		n := len(inFlightVec)
		entries := makeEntries(n)

		// Set InFlight atomically on each entry.
		for i, v := range inFlightVec {
			entries[i].InFlight.Store(v)
		}

		picker := newLRPicker(entries)
		result, err := picker.Pick(balancer.PickInfo{})
		if err != nil {
			rt.Fatalf("Pick returned unexpected error: %v", err)
		}

		// Find the expected minimum in-flight value.
		minInFlight := inFlightVec[0]
		for _, v := range inFlightVec[1:] {
			if v < minInFlight {
				minInFlight = v
			}
		}

		// Find all entries tied at minInFlight; the one with the lex-smallest
		// "address:port" must be the one that was chosen.
		var expectedEntry *SubConnEntry
		var expectedKey string
		for _, entry := range entries {
			// Compare against the stored value before the Pick incremented it.
			// After Pick, the chosen entry's InFlight has been bumped by 1; the
			// others remain unchanged. So we compare inFlightVec values.
			k := entry.Key.addrPort()
			v := inFlightVec[indexOf(entries, entry)]
			if v == minInFlight {
				if expectedEntry == nil || k < expectedKey {
					expectedEntry = entry
					expectedKey = k
				}
			}
		}

		if result.SubConn != expectedEntry.SubConn {
			rt.Fatalf("expected SubConn %s (min InFlight=%d), got a different SubConn",
				expectedKey, minInFlight)
		}

		// After Pick, the chosen entry's InFlight must be minInFlight+1.
		gotInFlight := expectedEntry.InFlight.Load()
		if gotInFlight != minInFlight+1 {
			rt.Fatalf("expected InFlight of chosen entry to be %d after Pick, got %d",
				minInFlight+1, gotInFlight)
		}
	})
}

// indexOf returns the position of entry in entries (used to read inFlightVec).
func indexOf(entries []*SubConnEntry, target *SubConnEntry) int {
	for i, e := range entries {
		if e == target {
			return i
		}
	}
	panic("entry not found")
}

// ---------------------------------------------------------------------------
// Task 11.7 – Unit tests for registry and error paths
// ---------------------------------------------------------------------------

// TestRegistryUnknownPolicy verifies that LookupPolicy for an unregistered
// name returns an error wrapping ErrUnknownPolicy.
func TestRegistryUnknownPolicy(t *testing.T) {
	_, err := LookupPolicy("unknown_xyz")
	if err == nil {
		t.Fatal("expected error for unknown policy, got nil")
	}
	if !errors.Is(err, ErrUnknownPolicy) {
		t.Errorf("expected error to wrap ErrUnknownPolicy, got: %v", err)
	}
}

// TestRegistryDuplicatePolicy verifies that re-registering a built-in policy
// name returns an error wrapping ErrPolicyExists.
func TestRegistryDuplicatePolicy(t *testing.T) {
	err := RegisterPolicy("round_robin", newRRPicker)
	if err == nil {
		t.Fatal("expected error for duplicate policy registration, got nil")
	}
	if !errors.Is(err, ErrPolicyExists) {
		t.Errorf("expected error to wrap ErrPolicyExists, got: %v", err)
	}
}

// TestRegistryCustomPolicy verifies that a new custom policy can be registered
// and then looked up successfully.
func TestRegistryCustomPolicy(t *testing.T) {
	const name = "my_custom_policy_test"

	// Clean up after the test so other tests are not affected.
	t.Cleanup(func() {
		registryMu.Lock()
		defer registryMu.Unlock()
		delete(registry, name)
	})

	factory := func(healthy []*SubConnEntry) balancer.Picker {
		return newRRPicker(healthy)
	}

	if err := RegisterPolicy(name, factory); err != nil {
		t.Fatalf("RegisterPolicy(%q) unexpected error: %v", name, err)
	}

	got, err := LookupPolicy(name)
	if err != nil {
		t.Fatalf("LookupPolicy(%q) unexpected error: %v", name, err)
	}
	if got == nil {
		t.Fatal("LookupPolicy returned nil factory")
	}
}

// TestNoHealthyPicker verifies that noHealthyPicker.Pick returns
// codes.Unavailable with a message containing "no healthy subconns".
func TestNoHealthyPicker(t *testing.T) {
	p := &noHealthyPicker{target: "test"}

	_, err := p.Pick(balancer.PickInfo{})
	if err == nil {
		t.Fatal("expected error from noHealthyPicker.Pick, got nil")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.Unavailable {
		t.Errorf("expected code Unavailable, got %s", st.Code())
	}
	if msg := st.Message(); len(msg) == 0 {
		t.Error("expected non-empty error message")
	} else {
		const want = "no healthy subconns"
		if !containsSubstring(msg, want) {
			t.Errorf("expected message to contain %q, got %q", want, msg)
		}
	}
}

// containsSubstring reports whether s contains substr (simple linear scan to
// avoid importing "strings" for a single use).
func containsSubstring(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(substr) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestPickerExclusion verifies that both rrPicker and lrPicker exclude already
// tried backends when alternatives exist (R7.3).
func TestPickerExclusion(t *testing.T) {
	// Create two backend entries: 10.0.0.1 and 10.0.0.2.
	entries := makeEntries(2)
	
	// Create AttemptTracker and add 10.0.0.1:8080 to it.
	tracker := &AttemptTracker{
		Addrs: []string{"10.0.0.1:8080"},
	}
	
	ctx := context.WithValue(context.Background(), TrackerKey, tracker)
	pickInfo := balancer.PickInfo{Ctx: ctx}

	// 1. Test rrPicker exclusion
	rr := newRRPicker(entries)
	res, err := rr.Pick(pickInfo)
	if err != nil {
		t.Fatalf("rrPicker.Pick unexpected error: %v", err)
	}
	// It must choose 10.0.0.2:8080 because 10.0.0.1:8080 is excluded.
	if entries[1].SubConn != res.SubConn {
		t.Errorf("rrPicker did not respect exclusion: got SubConn from %v, want from %v", res.SubConn, entries[1].SubConn)
	}

	// 2. Test lrPicker exclusion
	tracker2 := &AttemptTracker{
		Addrs: []string{"10.0.0.1:8080"},
	}
	ctx2 := context.WithValue(context.Background(), TrackerKey, tracker2)
	pickInfo2 := balancer.PickInfo{Ctx: ctx2}

	lr := newLRPicker(entries)
	res, err = lr.Pick(pickInfo2)
	if err != nil {
		t.Fatalf("lrPicker.Pick unexpected error: %v", err)
	}
	// It must choose 10.0.0.2:8080 because 10.0.0.1:8080 is excluded.
	if entries[1].SubConn != res.SubConn {
		t.Errorf("lrPicker did not respect exclusion: got SubConn from %v, want from %v", res.SubConn, entries[1].SubConn)
	}
}


