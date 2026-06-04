package grpck8sbalancer

import (
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name        string
		cfg         func() *Config
		wantErr     bool
		errContains string
		isUnknown   bool // check errors.Is(err, ErrUnknownPolicy)
	}{
		{
			name:    "DefaultConfig passes validate",
			cfg:     DefaultConfig,
			wantErr: false,
		},
		{
			name: "unknown LoadBalancingPolicy",
			cfg: func() *Config {
				c := DefaultConfig()
				c.LoadBalancingPolicy = "some_unknown_policy"
				return c
			},
			wantErr:     true,
			errContains: "LoadBalancingPolicy",
			isUnknown:   true,
		},
		{
			name: "RetryPolicy.MaxAttempts = 0",
			cfg: func() *Config {
				c := DefaultConfig()
				c.RetryPolicy.MaxAttempts = 0
				return c
			},
			wantErr:     true,
			errContains: "MaxAttempts",
		},
		{
			name: "RetryPolicy.InitialBackoff = -1ms",
			cfg: func() *Config {
				c := DefaultConfig()
				c.RetryPolicy.InitialBackoff = -1 * time.Millisecond
				return c
			},
			wantErr:     true,
			errContains: "InitialBackoff",
		},
		{
			name: "RetryPolicy.MaxBackoff = -1ms",
			cfg: func() *Config {
				c := DefaultConfig()
				c.RetryPolicy.MaxBackoff = -1 * time.Millisecond
				return c
			},
			wantErr:     true,
			errContains: "MaxBackoff",
		},
		{
			name: "RetryPolicy.BackoffMultiplier = 0.5",
			cfg: func() *Config {
				c := DefaultConfig()
				c.RetryPolicy.BackoffMultiplier = 0.5
				return c
			},
			wantErr:     true,
			errContains: "BackoffMultiplier",
		},
		{
			name: "MaxSubconnections = -1",
			cfg: func() *Config {
				c := DefaultConfig()
				c.MaxSubconnections = -1
				return c
			},
			wantErr:     true,
			errContains: "MaxSubconnections",
		},
		{
			name: "MaxAttempts = 2 with empty RetryableStatusCodes",
			cfg: func() *Config {
				c := DefaultConfig()
				c.RetryPolicy.MaxAttempts = 2
				c.RetryPolicy.RetryableStatusCodes = []codes.Code{}
				return c
			},
			wantErr:     true,
			errContains: "RetryableStatusCodes",
		},
		{
			name: "HealthCheckInterval = -1ms",
			cfg: func() *Config {
				c := DefaultConfig()
				c.HealthCheckInterval = -1 * time.Millisecond
				return c
			},
			wantErr:     true,
			errContains: "HealthCheckInterval",
		},
		{
			name: "HealthCheckTimeout = -1ms",
			cfg: func() *Config {
				c := DefaultConfig()
				c.HealthCheckTimeout = -1 * time.Millisecond
				return c
			},
			wantErr:     true,
			errContains: "HealthCheckTimeout",
		},
		{
			name: "ResolverGraceWindow = -1ms",
			cfg: func() *Config {
				c := DefaultConfig()
				c.ResolverGraceWindow = -1 * time.Millisecond
				return c
			},
			wantErr:     true,
			errContains: "ResolverGraceWindow",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg()
			err := cfg.validate()

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				if tt.isUnknown && !errors.Is(err, ErrUnknownPolicy) {
					t.Errorf("expected errors.Is(err, ErrUnknownPolicy) to be true, got false; err=%v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}
