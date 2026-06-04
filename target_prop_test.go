package grpck8sbalancer

import (
	"testing"

	"pgregory.net/rapid"
)

// k8sNameRunes is the set of characters allowed after the first character of a
// Kubernetes name: lowercase letters, digits, and hyphens.
var k8sNameRunes = []rune("abcdefghijklmnopqrstuvwxyz0123456789-")

// genK8sName generates a valid Kubernetes name component: starts with a
// lowercase letter, followed by lowercase letters, digits, or hyphens, total
// 1–maxLen chars.
func genK8sName(t *rapid.T, maxLen int) string {
	// First character: a single lowercase ASCII letter.
	firstRune := rapid.RuneFrom([]rune("abcdefghijklmnopqrstuvwxyz")).Draw(t, "first")
	if maxLen == 1 {
		return string(firstRune)
	}
	// Remaining characters (0 to maxLen-1 additional): lowercase letters, digits, or hyphens.
	// StringOfN(elem, minRunes, maxRunes, maxLen) — pass -1 for no byte limit.
	rest := rapid.StringOfN(
		rapid.RuneFrom(k8sNameRunes),
		0, maxLen-1, -1,
	).Draw(t, "rest")
	return string(firstRune) + rest
}

// genPortName generates a valid Kubernetes port name: starts with a lowercase
// letter, followed by lowercase letters, digits, or hyphens, 1–15 chars total.
func genPortName(t *rapid.T) string {
	firstRune := rapid.RuneFrom([]rune("abcdefghijklmnopqrstuvwxyz")).Draw(t, "portFirst")
	rest := rapid.StringOfN(
		rapid.RuneFrom(k8sNameRunes),
		0, 14, -1,
	).Draw(t, "portRest")
	combined := string(firstRune) + rest
	// Safety clamp: ensure we never exceed 15 characters.
	if len(combined) > 15 {
		combined = combined[:15]
	}
	return combined
}

// genPort generates either a numeric Port (Number ∈ [1, 65535]) or a named Port.
func genPort(t *rapid.T) Port {
	if rapid.Bool().Draw(t, "numericPort") {
		n := rapid.IntRange(1, 65535).Draw(t, "portNumber")
		return Port{Number: n}
	}
	return Port{Name: genPortName(t)}
}

// genK8sTarget generates a valid Target with Scheme="k8s".
func genK8sTarget(t *rapid.T) Target {
	ns := genK8sName(t, 63)
	svc := genK8sName(t, 63)
	port := genPort(t)
	return Target{
		Scheme:    "k8s",
		Namespace: ns,
		Service:   svc,
		Port:      port,
	}
}

// genDNSTarget generates a valid Target with Scheme="dns".
// The hostname follows the same valid-name rules as a k8s service name so that
// ParseTarget can round-trip it without the colon/slash characters that would
// confuse the URI tokenizer.
func genDNSTarget(t *rapid.T) Target {
	host := genK8sName(t, 63)
	port := genPort(t)
	return Target{
		Scheme:  "dns",
		Service: host,
		Port:    port,
	}
}

// genTarget generates a valid Target for either scheme.
func genTarget(t *rapid.T) Target {
	if rapid.Bool().Draw(t, "scheme") {
		return genK8sTarget(t)
	}
	return genDNSTarget(t)
}

// TestTargetRoundTrip verifies Property P1:
//
//	ParseTarget(t.String(), t.Namespace) == t
//
// for all valid Target values across both schemes.
//
// Validates: Requirements 2.5
func TestTargetRoundTrip(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		target := genTarget(rt)

		// --- first round-trip ---
		printed := target.String()
		parsed, err := ParseTarget(printed, target.Namespace)
		if err != nil {
			rt.Fatalf("ParseTarget(%q, %q) returned unexpected error: %v", printed, target.Namespace, err)
		}
		if parsed != target {
			rt.Fatalf(
				"round-trip mismatch:\n  original: %+v\n  printed:  %q\n  reparsed: %+v",
				target, printed, parsed,
			)
		}

		// --- second round-trip: re-print and re-parse ---
		printed2 := parsed.String()
		parsed2, err := ParseTarget(printed2, parsed.Namespace)
		if err != nil {
			rt.Fatalf("second ParseTarget(%q, %q) returned unexpected error: %v", printed2, parsed.Namespace, err)
		}
		if parsed2 != parsed {
			rt.Fatalf(
				"second round-trip mismatch:\n  first parse: %+v\n  printed:     %q\n  re-parsed:   %+v",
				parsed, printed2, parsed2,
			)
		}
	})
}
