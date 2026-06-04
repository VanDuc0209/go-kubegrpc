package retry

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/balancer"
)


// Policy carries the retry configuration needed by the interceptors.
// It maps 1-to-1 onto Config.RetryPolicy in the root package.
// Defined here rather than imported from the root to avoid an import cycle.
type Policy struct {
	MaxAttempts          int
	InitialBackoff       time.Duration
	MaxBackoff           time.Duration
	BackoffMultiplier    float64
	RetryableStatusCodes []codes.Code
}

// isRetryable reports whether err corresponds to one of the configured
// retryable status codes.
func isRetryable(err error, retryable []codes.Code) bool {
	if err == nil {
		return false
	}
	c := status.Code(err)
	for _, rc := range retryable {
		if c == rc {
			return true
		}
	}
	return false
}

// UnaryClientInterceptor returns a grpc.UnaryClientInterceptor that retries
// failed unary RPCs according to policy.
//
// isNonIdempotentFn is supplied by the root package (grpck8sbalancer.IsNonIdempotent)
// as a function value to avoid an import cycle between internal/retry and the
// root package.
//
// Requirements: R7.1, R7.2, R7.5, R7.6, R7.7, R7.8
func UnaryClientInterceptor(
	policy Policy,
	isNonIdempotentFn func([]grpc.CallOption) bool,
	clock Clock,
) grpc.UnaryClientInterceptor {
	if clock == nil {
		clock = realClock{}
	}

	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		// R7.7: do not retry non-idempotent RPCs.
		if isNonIdempotentFn != nil && isNonIdempotentFn(opts) {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		// R7.8: do not retry when MaxAttempts <= 1.
		if policy.MaxAttempts <= 1 {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		tracker := &balancer.AttemptTracker{}
		ctx = context.WithValue(ctx, balancer.TrackerKey, tracker)

		var lastErr error
		for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
			// R7.5: bail out immediately if the context is already cancelled.
			if ctx.Err() != nil {
				if lastErr != nil {
					return fmt.Errorf("%w: %w", ctx.Err(), lastErr)
				}
				return ctx.Err()
			}

			lastErr = invoker(ctx, method, req, reply, cc, opts...)
			if lastErr == nil {
				return nil
			}

			// R7.6: non-retryable code — return immediately.
			if !isRetryable(lastErr, policy.RetryableStatusCodes) {
				return lastErr
			}

			// Exhausted all attempts.
			if attempt >= policy.MaxAttempts {
				break
			}

			// R7.4: sleep for the computed backoff, respecting context cancellation.
			backoff := AttemptBackoff(attempt, policy.InitialBackoff, policy.MaxBackoff, policy.BackoffMultiplier)
			select {
			case <-ctx.Done():
				// R7.5: context expired during sleep.
				return fmt.Errorf("%w: %w", ctx.Err(), lastErr)
			case <-clock.After(backoff):
				// proceed to the next attempt
			}
		}

		return lastErr
	}
}

// retryableClientStream wraps grpc.ClientStream and tracks whether a message
// has been successfully sent. Once SendMsg succeeds, retry is disabled for
// that stream to preserve message-ordering invariants.
type retryableClientStream struct {
	grpc.ClientStream
	// sentMsg is set to true after the first successful SendMsg call.
	sentMsg bool
}

// SendMsg forwards the message and marks the stream as non-retryable on success.
func (s *retryableClientStream) SendMsg(m any) error {
	err := s.ClientStream.SendMsg(m)
	if err == nil {
		s.sentMsg = true
	}
	return err
}

// StreamClientInterceptor returns a grpc.StreamClientInterceptor that retries
// stream establishment failures according to policy.
//
// Retry is only attempted before the first SendMsg succeeds on a stream. After
// that point the stream is non-retryable.
//
// Requirements: R7.2, R7.6, R7.7, R7.8
func StreamClientInterceptor(
	policy Policy,
	isNonIdempotentFn func([]grpc.CallOption) bool,
	clock Clock,
) grpc.StreamClientInterceptor {
	if clock == nil {
		clock = realClock{}
	}

	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		// R7.7: do not retry non-idempotent RPCs.
		if isNonIdempotentFn != nil && isNonIdempotentFn(opts) {
			return streamer(ctx, desc, cc, method, opts...)
		}

		// R7.8: do not retry when MaxAttempts <= 1.
		if policy.MaxAttempts <= 1 {
			return streamer(ctx, desc, cc, method, opts...)
		}

		tracker := &balancer.AttemptTracker{}
		ctx = context.WithValue(ctx, balancer.TrackerKey, tracker)

		var (
			lastErr    error
			lastStream *retryableClientStream
		)

		for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
			// R7.5: bail out immediately if the context is already cancelled.
			if ctx.Err() != nil {
				if lastErr != nil {
					return nil, fmt.Errorf("%w: %w", ctx.Err(), lastErr)
				}
				return nil, ctx.Err()
			}

			// Once a message has been sent on a previous stream attempt, the
			// stream is no longer retryable regardless of subsequent errors.
			if lastStream != nil && lastStream.sentMsg {
				return lastStream, lastErr
			}

			var rawStream grpc.ClientStream
			rawStream, lastErr = streamer(ctx, desc, cc, method, opts...)
			if lastErr == nil {
				wrapped := &retryableClientStream{ClientStream: rawStream}
				lastStream = wrapped
				return wrapped, nil
			}

			// R7.6: non-retryable code — return immediately.
			if !isRetryable(lastErr, policy.RetryableStatusCodes) {
				return nil, lastErr
			}

			// Exhausted all attempts.
			if attempt >= policy.MaxAttempts {
				break
			}

			// R7.4: sleep for the computed backoff, respecting context cancellation.
			backoff := AttemptBackoff(attempt, policy.InitialBackoff, policy.MaxBackoff, policy.BackoffMultiplier)
			select {
			case <-ctx.Done():
				// R7.5: context expired during sleep.
				return nil, fmt.Errorf("%w: %w", ctx.Err(), lastErr)
			case <-clock.After(backoff):
				// proceed to the next attempt
			}
		}

		return nil, lastErr
	}
}
