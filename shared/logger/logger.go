package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New creates a new Zap logger with the given level
func New(level string) (*zap.Logger, error) {
	var zapLevel zapcore.Level
	if err := zapLevel.UnmarshalText([]byte(level)); err != nil {
		zapLevel = zapcore.InfoLevel
	}

	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zapLevel)
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.CallerKey = "caller"
	config.EncoderConfig.StacktraceKey = "stacktrace"

	logger, err := config.Build(
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)
	if err != nil {
		return nil, err
	}

	return logger, nil
}

// NewDevelopment creates a new development logger with human-readable output
func NewDevelopment() (*zap.Logger, error) {
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, err := config.Build(
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)
	if err != nil {
		return nil, err
	}

	return logger, nil
}

// WithFields adds contextual fields to a logger
func WithFields(logger *zap.Logger, fields ...zap.Field) *zap.Logger {
	return logger.With(fields...)
}

// WithRequestID adds a request ID to the logger
func WithRequestID(logger *zap.Logger, requestID string) *zap.Logger {
	return logger.With(zap.String("request_id", requestID))
}

// WithUserID adds a user ID to the logger
func WithUserID(logger *zap.Logger, userID string) *zap.Logger {
	return logger.With(zap.String("user_id", userID))
}

// WithOrderID adds an order ID to the logger
func WithOrderID(logger *zap.Logger, orderID string) *zap.Logger {
	return logger.With(zap.String("order_id", orderID))
}

// Sync flushes any buffered log entries
func Sync(logger *zap.Logger) error {
	return logger.Sync()
}

// Must is a helper that wraps a call to New and panics if there is an error
func Must(logger *zap.Logger, err error) *zap.Logger {
	if err != nil {
		panic(err)
	}
	return logger
}

// SugaredLogger returns a sugared logger for less structured logging
func SugaredLogger(logger *zap.Logger) *zap.SugaredLogger {
	return logger.Sugar()
}

// NewNopLogger returns a no-op logger that discards all logs
func NewNopLogger() *zap.Logger {
	return zap.NewNop()
}

// RedirectStdLog redirects the standard library's log package to use Zap
func RedirectStdLog(logger *zap.Logger) {
	zap.RedirectStdLog(logger)
}

// StdLogSink returns a zapcore.WriteSyncer that writes to stdout
func StdLogSink() zapcore.WriteSyncer {
	return os.Stdout
}

// StdErrLogSink returns a zapcore.WriteSyncer that writes to stderr
func StdErrLogSink() zapcore.WriteSyncer {
	return os.Stderr
}

// NewFileLogger creates a logger that writes to a file
func NewFileLogger(filePath string, level string) (*zap.Logger, error) {
	var zapLevel zapcore.Level
	if err := zapLevel.UnmarshalText([]byte(level)); err != nil {
		zapLevel = zapcore.InfoLevel
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	writeSyncer := zapcore.AddSync(file)
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		writeSyncer,
		zapLevel,
	)

	return zap.New(core, zap.AddCaller()), nil
}
