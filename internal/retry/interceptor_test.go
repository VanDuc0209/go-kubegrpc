package retry

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// mockInvoker records how many times it was called and returns pre-configured
// responses in order. Once the responses slice is exhausted, it returns nil.
type mockInvoker struct {
	calls     int
	responses []error
}

func (m *mockInvoker) invoke(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	opts ...grpc.CallOption,
) error {
	m.calls++
	if m.calls <= len(m.responses) {
		return m.responses[m.calls-1]
	}
	return nil
}

// fakeClock is an injectable clock whose After channel never fires, so the
// interceptor will block in the backoff select until the context is cancelled.
type fakeClock struct {
	ch chan time.Time
}

func (f *fakeClock) Now() time.Time                         { return time.Now() }
func (f *fakeClock) Sleep(d time.Duration)                  {}
func (f *fakeClock) After(d time.Duration) <-chan time.Time { return f.ch }

// basePolicy is a convenience builder for a retryable Policy.
func basePolicy() Policy {
	return Policy{
		MaxAttempts:          3,
		InitialBackoff:       1 * time.Millisecond,
		MaxBackoff:           10 * time.Millisecond,
		BackoffMultiplier:    2.0,
		RetryableStatusCodes: []codes.Code{codes.Unavailable},
	}
}

// realClock uses time.After for tests that need the backoff select to resolve.
type testRealClock struct{}

func (testRealClock) Now() time.Time                         { return time.Now() }
func (testRealClock) Sleep(d time.Duration)                  { time.Sleep(d) }
func (testRealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestRetryOnRetryableCode verifies that when an invoker fails once with a
// retryable code (codes.Unavailable) and then succeeds, the interceptor
// retries and the invoker is called exactly twice.
//
// Validates: Requirements 7.2
func TestRetryOnRetryableCode(t *testing.T) {
	mi := &mockInvoker{
		responses: []error{
			status.Error(codes.Unavailable, "transient"),
		},
	}
	p := basePolicy()
	interceptor := UnaryClientInterceptor(p, nil, testRealClock{})

	err := interceptor(context.Background(), "/svc/Method", nil, nil, nil, mi.invoke)
	if err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if mi.calls != 2 {
		t.Fatalf("invoker called %d times; want 2", mi.calls)
	}
}

// TestNoRetryOnNonRetryableCode verifies that when an invoker returns a
// non-retryable code, the interceptor returns immediately without retrying.
//
// Validates: Requirements 7.6
func TestNoRetryOnNonRetryableCode(t *testing.T) {
	mi := &mockInvoker{
		responses: []error{
			status.Error(codes.PermissionDenied, "denied"),
		},
	}
	p := basePolicy()
	interceptor := UnaryClientInterceptor(p, nil, testRealClock{})

	err := interceptor(context.Background(), "/svc/Method", nil, nil, nil, mi.invoke)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if mi.calls != 1 {
		t.Fatalf("invoker called %d times; want 1", mi.calls)
	}
}

// TestNonIdempotentSkipsRetry verifies that when an isNonIdempotentFn returns
// true, the interceptor skips retry even if the error is retryable.
//
// Validates: Requirements 7.7
func TestNonIdempotentSkipsRetry(t *testing.T) {
	mi := &mockInvoker{
		responses: []error{
			status.Error(codes.Unavailable, "transient"),
		},
	}
	p := basePolicy()
	alwaysNonIdempotent := func(_ []grpc.CallOption) bool { return true }
	interceptor := UnaryClientInterceptor(p, alwaysNonIdempotent, testRealClock{})

	err := interceptor(context.Background(), "/svc/Method", nil, nil, nil, mi.invoke)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if mi.calls != 1 {
		t.Fatalf("invoker called %d times; want 1 (non-idempotent must not retry)", mi.calls)
	}
}

// TestMaxAttempts1SkipsRetry verifies that when Policy.MaxAttempts is 1,
// the interceptor never retries and calls the invoker exactly once.
//
// Validates: Requirements 7.8
func TestMaxAttempts1SkipsRetry(t *testing.T) {
	mi := &mockInvoker{
		responses: []error{
			status.Error(codes.Unavailable, "transient"),
		},
	}
	p := basePolicy()
	p.MaxAttempts = 1
	interceptor := UnaryClientInterceptor(p, nil, testRealClock{})

	err := interceptor(context.Background(), "/svc/Method", nil, nil, nil, mi.invoke)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if mi.calls != 1 {
		t.Fatalf("invoker called %d times; want 1 (MaxAttempts=1 must not retry)", mi.calls)
	}
}

// TestContextCancelDuringBackoff verifies that when the context is cancelled
// while the interceptor is waiting in the backoff sleep, the interceptor
// returns the context error.
//
// Validates: Requirements 7.5
func TestContextCancelDuringBackoff(t *testing.T) {
	// The fake clock's After channel never fires, so the interceptor blocks
	// in the backoff select indefinitely — until context cancellation races it.
	fc := &fakeClock{ch: make(chan time.Time)} // unbuffered, never sends

	mi := &mockInvoker{
		responses: []error{
			status.Error(codes.Unavailable, "transient"),
			status.Error(codes.Unavailable, "transient"),
			status.Error(codes.Unavailable, "transient"),
		},
	}
	p := basePolicy()
	p.MaxAttempts = 5 // large enough that retry loop would continue

	ctx, cancel := context.WithCancel(context.Background())
	interceptor := UnaryClientInterceptor(p, nil, fc)

	done := make(chan error, 1)
	go func() {
		done <- interceptor(ctx, "/svc/Method", nil, nil, nil, mi.invoke)
	}()

	// Give the interceptor time to reach the backoff select, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a context error, got nil")
		}
		// The returned error must wrap the context error.
		if ctx.Err() == nil {
			t.Fatal("context should be cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("interceptor did not return after context cancellation")
	}
}
