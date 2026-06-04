package observability

import (
	"log/slog"
)

// Logger wraps slog.Logger and provides convenience methods for the library.
type Logger struct {
	logger *slog.Logger
}

// NewLogger creates a Logger. If logger is nil, uses slog.Default() (R9.6).
func NewLogger(logger *slog.Logger) *Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return &Logger{logger: logger}
}

// LogRetry emits a DEBUG log for each retry attempt (R10.5).
// Fields: method, attempt, prior_code, endpoint.
func (l *Logger) LogRetry(method string, attempt int, priorCode string, endpoint string) {
	l.logger.Debug("rpc retry attempt",
		slog.String("method", method),
		slog.Int("attempt", attempt),
		slog.String("prior_code", priorCode),
		slog.String("endpoint", endpoint),
	)
}

// LogHealthTransition emits an INFO log on SubConn health state changes (R10.6).
// Fields: endpoint, state.
func (l *Logger) LogHealthTransition(endpoint, state string) {
	l.logger.Info("subconn health state changed",
		slog.String("endpoint", endpoint),
		slog.String("state", state),
	)
}

// LogSubConnCreated emits an INFO log when a SubConn is created (R4.7).
// Fields: endpoint, port, reason.
func (l *Logger) LogSubConnCreated(endpoint string, port int, reason string) {
	l.logger.Info("subconn created",
		slog.String("endpoint", endpoint),
		slog.Int("port", port),
		slog.String("reason", reason),
	)
}

// LogSubConnClosed emits an INFO log when a SubConn is closed (R4.7).
// Fields: endpoint, port, reason.
func (l *Logger) LogSubConnClosed(endpoint string, port int, reason string) {
	l.logger.Info("subconn closed",
		slog.String("endpoint", endpoint),
		slog.Int("port", port),
		slog.String("reason", reason),
	)
}
