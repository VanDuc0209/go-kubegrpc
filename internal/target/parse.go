// Package target provides internal string-manipulation helpers for parsing
// gRPC target URIs. It deliberately avoids importing the root package to
// prevent import cycles; the root package imports this package and wraps the
// results in its own public types.
package target

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Port represents either a numeric port number or a named port.
// Exactly one of Number or Name is non-zero/non-empty.
type Port struct {
	// Name is non-empty when this is a named port.
	Name string
	// Number is non-zero when this is a numeric port (1..65535).
	Number int
}

// Sentinel errors used only within this package. The root package maps these
// to its own exported sentinel errors via errors.Is-compatible wrapping.
var (
	// ErrUnsupportedScheme is returned when the scheme is neither "k8s" nor "dns".
	ErrUnsupportedScheme = errors.New("unsupported scheme")

	// ErrMissingNamespace is returned when the k8s path omits the namespace and
	// no default is provided.
	ErrMissingNamespace = errors.New("missing namespace")

	// ErrTargetInvalid is returned for malformed targets (empty service, bad port, etc.).
	ErrTargetInvalid = errors.New("invalid target")
)

// numericPortRe matches strings that consist entirely of ASCII digits.
var numericPortRe = regexp.MustCompile(`^[0-9]+$`)

// SplitScheme splits a URI of the form "<scheme>:///<rest>" into the scheme
// and the authority+path portion that follows "://". The three-slash form
// ("k8s:///...") is conventional for gRPC targets with no authority; the
// function accepts any number of slashes after "://" and returns everything
// after the "://" prefix as rest (including the leading slashes).
//
// Returns ErrTargetInvalid if the string does not contain "://".
func SplitScheme(s string) (scheme, rest string, err error) {
	idx := strings.Index(s, "://")
	if idx < 0 {
		return "", "", fmt.Errorf("no scheme separator \"://\" in %q: %w", s, ErrTargetInvalid)
	}
	scheme = s[:idx]
	rest = s[idx+len("://"):]
	return scheme, rest, nil
}

// ParsePort parses a port token into a Port.
//   - If the token matches ^[0-9]+$, it is treated as numeric: parsed to int,
//     validated to be in [1..65535], and returned as Port{Number: n}.
//   - Otherwise it is treated as a named port and returned as Port{Name: s}.
//   - An empty token is always an error.
func ParsePort(s string) (Port, error) {
	if s == "" {
		return Port{}, fmt.Errorf("port must not be empty: %w", ErrTargetInvalid)
	}
	if numericPortRe.MatchString(s) {
		n, err := strconv.Atoi(s)
		if err != nil {
			// Atoi should never fail for a string of digits, but handle defensively.
			return Port{}, fmt.Errorf("port %q is not a valid integer: %w", s, ErrTargetInvalid)
		}
		if n < 1 || n > 65535 {
			return Port{}, fmt.Errorf("port %d is out of range [1..65535]: %w", n, ErrTargetInvalid)
		}
		return Port{Number: n}, nil
	}
	return Port{Name: s}, nil
}

// SplitK8sPath splits the path portion of a k8s URI (the part after "://")
// into namespace, service, and port components.
//
// Accepted forms (where path begins with one or more '/'):
//
//	/<namespace>/<service>:<port>  →  namespace="<namespace>", service="<service>", portStr="<port>"
//	/<service>:<port>              →  namespace="", service="<service>", portStr="<port>"
//
// The function strips any leading slashes that follow "://" (i.e. it handles
// both "k8s:///<ns>/<svc>:<port>" and the raw path "/<ns>/<svc>:<port>").
// It returns ErrTargetInvalid when the service or port segment is absent.
func SplitK8sPath(path string) (namespace, service, portStr string, err error) {
	// Strip all leading slashes (the authority section is empty for gRPC k8s targets).
	trimmed := strings.TrimLeft(path, "/")

	// The remaining string must contain a colon separating service from port.
	// Find the last colon so that IPv6-style addresses (future) are handled
	// gracefully — for now the service name cannot contain colons anyway.
	colonIdx := strings.LastIndex(trimmed, ":")
	if colonIdx < 0 {
		return "", "", "", fmt.Errorf("k8s target path %q is missing a port (expected \"[<ns>/]<svc>:<port>\"): %w", path, ErrTargetInvalid)
	}
	portStr = trimmed[colonIdx+1:]
	serviceAndNS := trimmed[:colonIdx]

	// serviceAndNS is now either "<ns>/<svc>" or "<svc>".
	slashIdx := strings.Index(serviceAndNS, "/")
	if slashIdx >= 0 {
		namespace = serviceAndNS[:slashIdx]
		service = serviceAndNS[slashIdx+1:]
	} else {
		namespace = ""
		service = serviceAndNS
	}

	if service == "" {
		return "", "", "", fmt.Errorf("k8s target path %q has an empty service name: %w", path, ErrTargetInvalid)
	}

	return namespace, service, portStr, nil
}

// SplitDNSPath splits the path portion of a dns URI into host and port.
//
//	/<host>:<port>  →  host="<host>", portStr="<port>"
//
// Returns ErrTargetInvalid when the host or port is absent.
func SplitDNSPath(path string) (host, portStr string, err error) {
	trimmed := strings.TrimLeft(path, "/")

	colonIdx := strings.LastIndex(trimmed, ":")
	if colonIdx < 0 {
		return "", "", fmt.Errorf("dns target path %q is missing a port (expected \"<host>:<port>\"): %w", path, ErrTargetInvalid)
	}
	portStr = trimmed[colonIdx+1:]
	host = trimmed[:colonIdx]

	if host == "" {
		return "", "", fmt.Errorf("dns target path %q has an empty host: %w", path, ErrTargetInvalid)
	}

	return host, portStr, nil
}

// PortString formats a Port back to its canonical string representation:
// numeric ports are formatted as their decimal value, named ports as their name.
func PortString(p Port) string {
	if p.Number != 0 {
		return strconv.Itoa(p.Number)
	}
	return p.Name
}
