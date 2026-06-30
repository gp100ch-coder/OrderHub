package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

// Circuit breaker states
const (
	StateClosed   = "closed"
	StateOpen     = "open"
	StateHalfOpen = "half_open"
)

// Error definitions
var (
	ErrCircuitOpen = errors.New("circuit breaker is open")
)

// Config holds circuit breaker configuration
type Config struct {
	MaxFailures   int           // Number of failures before opening circuit
	Timeout       time.Duration // Time to wait before transitioning to half-open
	HalfOpenLimit int           // Number of successful calls in half-open state to close circuit
}

// DefaultConfig returns default circuit breaker configuration
func DefaultConfig() Config {
	return Config{
		MaxFailures:   5,
		Timeout:       30 * time.Second,
		HalfOpenLimit: 3,
	}
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	mu            sync.RWMutex
	state         string
	failures      int
	successes     int
	lastFailure   time.Time
	maxFailures   int
	timeout       time.Duration
	halfOpenLimit int
}

// New creates a new CircuitBreaker with the given configuration
func New(config Config) *CircuitBreaker {
	if config.MaxFailures <= 0 {
		config.MaxFailures = 5
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	if config.HalfOpenLimit <= 0 {
		config.HalfOpenLimit = 3
	}

	return &CircuitBreaker{
		state:         StateClosed,
		maxFailures:   config.MaxFailures,
		timeout:       config.Timeout,
		halfOpenLimit: config.HalfOpenLimit,
	}
}

// Execute runs the given function with circuit breaker protection
func (cb *CircuitBreaker) Execute(fn func() error) error {
	if !cb.allowRequest() {
		return ErrCircuitOpen
	}

	err := fn()
	cb.recordResult(err)
	return err
}

// ExecuteWithFallback runs the primary function, and if it fails due to open circuit, runs fallback
func (cb *CircuitBreaker) ExecuteWithFallback(primary func() error, fallback func() error) error {
	err := cb.Execute(primary)
	if errors.Is(err, ErrCircuitOpen) && fallback != nil {
		return fallback()
	}
	return err
}

// State returns the current state of the circuit breaker
func (cb *CircuitBreaker) State() string {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// IsOpen returns true if the circuit is open
func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state == StateOpen
}

// Reset resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateClosed
	cb.failures = 0
	cb.successes = 0
}

func (cb *CircuitBreaker) allowRequest() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		// Check if timeout has elapsed
		if time.Since(cb.lastFailure) > cb.timeout {
			cb.state = StateHalfOpen
			cb.successes = 0
			return true
		}
		return false
	case StateHalfOpen:
		return true
	default:
		return false
	}
}

func (cb *CircuitBreaker) recordResult(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failures++
		cb.lastFailure = time.Now()

		switch cb.state {
		case StateClosed:
			if cb.failures >= cb.maxFailures {
				cb.state = StateOpen
			}
		case StateHalfOpen:
			// Any failure in half-open state opens the circuit again
			cb.state = StateOpen
		}
	} else {
		cb.successes++

		switch cb.state {
		case StateHalfOpen:
			if cb.successes >= cb.halfOpenLimit {
				cb.state = StateClosed
				cb.failures = 0
				cb.successes = 0
			}
		case StateClosed:
			// Reset failure count on success
			cb.failures = 0
		}
	}
}

// GetStats returns current statistics about the circuit breaker
func (cb *CircuitBreaker) GetStats() (state string, failures, successes int) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state, cb.failures, cb.successes
}
