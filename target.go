package grpck8sbalancer

import (
	"fmt"

	internaltarget "github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/target"
)

// Target identifies a logical service endpoint used by the library.
// Two schemes are supported:
//
//   - "k8s"  — a Kubernetes Service identified by namespace, service name, and port.
//   - "dns"  — a plain DNS hostname with a port (fallback / out-of-cluster use).
type Target struct {
	// Scheme is "k8s" or "dns".
	Scheme string

	// Namespace is the Kubernetes namespace (k8s scheme only; empty for dns).
	Namespace string

	// Service is the Kubernetes Service name (k8s) or the DNS hostname (dns).
	Service string

	// Port identifies the port by number or by name.
	Port Port
}

// Port identifies a network port either by its numeric value or by its
// Kubernetes Service port name. Exactly one of Number and Name should be set:
//
//   - Port{Number: 8080}  — numeric port
//   - Port{Name: "grpc"}  — named port (matches a port entry in the Service spec)
type Port struct {
	// Name is non-empty when this is a named port.
	Name string

	// Number is non-zero when this is a numeric port (valid range: 1..65535).
	Number int
}

// ParseTarget parses a target URI string into a Target.
//
// Accepted forms:
//
//	k8s:///<namespace>/<service>:<port>  — explicit namespace (R2.1)
//	k8s:///<service>:<port>              — namespace from defaultNamespace (R2.2)
//	dns:///<host>:<port>                 — DNS fallback
//
// Port may be numeric ("8080") or named ("grpc").
//
// Errors:
//   - ErrUnsupportedScheme — scheme is not "k8s" or "dns" (R2.6)
//   - ErrMissingNamespace  — k8s scheme, namespace absent, defaultNamespace is empty (R2.3)
//   - ErrTargetInvalid     — empty service/host, port out of range, or malformed URI
func ParseTarget(s string, defaultNamespace string) (Target, error) {
	scheme, rest, err := internaltarget.SplitScheme(s)
	if err != nil {
		return Target{}, fmt.Errorf("ParseTarget %q: %w", s, ErrTargetInvalid)
	}

	switch scheme {
	case "k8s":
		return parseK8sTarget(s, rest, defaultNamespace)
	case "dns":
		return parseDNSTarget(s, rest)
	default:
		return Target{}, fmt.Errorf("ParseTarget %q: scheme %q is not supported: %w", s, scheme, ErrUnsupportedScheme)
	}
}

// parseK8sTarget handles the "k8s" scheme branch of ParseTarget.
func parseK8sTarget(raw, rest, defaultNamespace string) (Target, error) {
	ns, svc, portStr, err := internaltarget.SplitK8sPath(rest)
	if err != nil {
		// SplitK8sPath returns ErrTargetInvalid-wrapped errors; translate them.
		return Target{}, fmt.Errorf("ParseTarget %q: %w", raw, ErrTargetInvalid)
	}

	// Resolve namespace: use explicit value if present, else fall back to default.
	if ns == "" {
		if defaultNamespace == "" {
			return Target{}, fmt.Errorf(
				"ParseTarget %q: namespace is absent from the target and Config.DefaultNamespace is empty: %w",
				raw, ErrMissingNamespace,
			)
		}
		ns = defaultNamespace
	}

	p, err := internaltarget.ParsePort(portStr)
	if err != nil {
		return Target{}, fmt.Errorf("ParseTarget %q: %w", raw, ErrTargetInvalid)
	}

	return Target{
		Scheme:    "k8s",
		Namespace: ns,
		Service:   svc,
		Port:      internalPortToPublic(p),
	}, nil
}

// parseDNSTarget handles the "dns" scheme branch of ParseTarget.
func parseDNSTarget(raw, rest string) (Target, error) {
	host, portStr, err := internaltarget.SplitDNSPath(rest)
	if err != nil {
		return Target{}, fmt.Errorf("ParseTarget %q: %w", raw, ErrTargetInvalid)
	}

	p, err := internaltarget.ParsePort(portStr)
	if err != nil {
		return Target{}, fmt.Errorf("ParseTarget %q: %w", raw, ErrTargetInvalid)
	}

	return Target{
		Scheme:  "dns",
		Service: host,
		Port:    internalPortToPublic(p),
	}, nil
}

// String formats the Target back into a canonical URI string.
//
// Forms produced:
//
//	k8s:///<namespace>/<service>:<port>  — when Namespace is non-empty
//	k8s:///<service>:<port>              — when Namespace is empty
//	dns:///<host>:<port>
//
// The output satisfies the round-trip property (R2.5):
//
//	ParseTarget(t.String(), t.Namespace) == t
func (t Target) String() string {
	portStr := internaltarget.PortString(publicPortToInternal(t.Port))

	switch t.Scheme {
	case "k8s":
		if t.Namespace != "" {
			return fmt.Sprintf("k8s:///%s/%s:%s", t.Namespace, t.Service, portStr)
		}
		return fmt.Sprintf("k8s:///%s:%s", t.Service, portStr)
	case "dns":
		return fmt.Sprintf("dns:///%s:%s", t.Service, portStr)
	default:
		// Produce a best-effort representation for unknown schemes rather than
		// returning an empty string that would be harder to debug.
		return fmt.Sprintf("%s:///%s:%s", t.Scheme, t.Service, portStr)
	}
}

// internalPortToPublic converts the internal/target Port type to the public Port type.
func internalPortToPublic(p internaltarget.Port) Port {
	return Port{
		Name:   p.Name,
		Number: p.Number,
	}
}

// publicPortToInternal converts the public Port type to the internal/target Port type.
func publicPortToInternal(p Port) internaltarget.Port {
	return internaltarget.Port{
		Name:   p.Name,
		Number: p.Number,
	}
}
