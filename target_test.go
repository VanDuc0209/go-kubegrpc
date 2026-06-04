package grpck8sbalancer

import (
	"errors"
	"testing"
)

// TestParseTargetErrors covers all error paths and selected success paths for
// ParseTarget, verifying that the correct sentinel errors are returned and that
// valid inputs produce the expected Target fields.
func TestParseTargetErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		input            string
		defaultNamespace string
		wantErr          error      // nil means success expected
		check            func(*testing.T, Target) // optional success assertions
	}{
		{
			// R2.6: scheme not "k8s" or "dns" must be rejected.
			name:    "unsupported scheme static",
			input:   "static://foo/bar:8080",
			wantErr: ErrUnsupportedScheme,
		},
		{
			// R2.3: k8s target without explicit namespace AND no defaultNamespace.
			name:             "missing namespace empty defaultNamespace",
			input:            "k8s:///myservice:8080",
			defaultNamespace: "",
			wantErr:          ErrMissingNamespace,
		},
		{
			// R2.2: k8s target without explicit namespace is resolved via defaultNamespace.
			name:             "missing namespace non-empty defaultNamespace fills in",
			input:            "k8s:///myservice:8080",
			defaultNamespace: "default",
			wantErr:          nil,
			check: func(t *testing.T, got Target) {
				t.Helper()
				if got.Namespace != "default" {
					t.Errorf("Namespace = %q; want %q", got.Namespace, "default")
				}
				if got.Service != "myservice" {
					t.Errorf("Service = %q; want %q", got.Service, "myservice")
				}
				if got.Port.Number != 8080 {
					t.Errorf("Port.Number = %d; want 8080", got.Port.Number)
				}
			},
		},
		{
			// Numeric port 0 is out of the valid range [1..65535].
			name:    "invalid numeric port zero",
			input:   "k8s:///ns/svc:0",
			wantErr: ErrTargetInvalid,
		},
		{
			// Numeric port 99999 exceeds the maximum valid port 65535.
			name:    "invalid numeric port 99999",
			input:   "k8s:///ns/svc:99999",
			wantErr: ErrTargetInvalid,
		},
		{
			// Empty service name after the namespace segment.
			name:    "empty service name",
			input:   "k8s:///ns/:8080",
			wantErr: ErrTargetInvalid,
		},
		{
			// No port separator at all.
			name:    "missing port",
			input:   "k8s:///ns/svc",
			wantErr: ErrTargetInvalid,
		},
		{
			// Happy-path: fully-qualified k8s target with explicit namespace.
			name:    "valid k8s full path",
			input:   "k8s:///ns/svc:8080",
			wantErr: nil,
			check: func(t *testing.T, got Target) {
				t.Helper()
				if got.Scheme != "k8s" {
					t.Errorf("Scheme = %q; want %q", got.Scheme, "k8s")
				}
				if got.Namespace != "ns" {
					t.Errorf("Namespace = %q; want %q", got.Namespace, "ns")
				}
				if got.Service != "svc" {
					t.Errorf("Service = %q; want %q", got.Service, "svc")
				}
				if got.Port.Number != 8080 {
					t.Errorf("Port.Number = %d; want 8080", got.Port.Number)
				}
			},
		},
		{
			// Happy-path: dns target with a named (non-numeric) port.
			name:    "valid dns named port",
			input:   "dns:///myhost:grpc",
			wantErr: nil,
			check: func(t *testing.T, got Target) {
				t.Helper()
				if got.Scheme != "dns" {
					t.Errorf("Scheme = %q; want %q", got.Scheme, "dns")
				}
				if got.Service != "myhost" {
					t.Errorf("Service = %q; want %q", got.Service, "myhost")
				}
				if got.Port.Name != "grpc" {
					t.Errorf("Port.Name = %q; want %q", got.Port.Name, "grpc")
				}
				if got.Port.Number != 0 {
					t.Errorf("Port.Number = %d; want 0 for named port", got.Port.Number)
				}
			},
		},
	}

	for _, tc := range tests {
		tc := tc // capture range variable for parallel sub-tests
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseTarget(tc.input, tc.defaultNamespace)

			if tc.wantErr != nil {
				if err == nil {
					t.Fatalf("ParseTarget(%q, %q) = %+v, nil; want error wrapping %v",
						tc.input, tc.defaultNamespace, got, tc.wantErr)
				}
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("ParseTarget(%q, %q) error = %v; want errors.Is(..., %v) to be true",
						tc.input, tc.defaultNamespace, err, tc.wantErr)
				}
				return
			}

			// Success path.
			if err != nil {
				t.Fatalf("ParseTarget(%q, %q) unexpected error: %v",
					tc.input, tc.defaultNamespace, err)
			}
			if tc.check != nil {
				tc.check(t, got)
			}
		})
	}
}
