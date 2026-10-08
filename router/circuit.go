package router

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/suhwr/blobkit"
)

// CircuitState represents the operational state of a circuit breaker.
type CircuitState string

const (
	// CircuitClosed routes traffic to the primary backend normally.
	CircuitClosed CircuitState = "closed"

	// CircuitOpen fast-fails or diverts to fallback after exceeding failure thresholds.
	CircuitOpen CircuitState = "open"

	// CircuitHalfOpen admits a bounded number of canary probes to test recovery.
	CircuitHalfOpen CircuitState = "half-open"
)

// CircuitBreakerConfig tunes failure detection, cooldown, and canary recovery thresholds.
type CircuitBreakerConfig struct {
	Primary          blobkit.Driver
	Fallback         blobkit.Driver
	FailureThreshold int
	SuccessThreshold int
	Cooldown         time.Duration
}

// CircuitBreakerRouter wraps primary and fallback drivers in an automated 3-state circuit breaker.
type CircuitBreakerRouter struct {
	primary  blobkit.Driver
	fallback blobkit.Driver

	failureThreshold int
	successThreshold int
	cooldown         time.Duration

	mu               sync.RWMutex
	state            CircuitState
	consecutiveFails int
	consecutiveWins  int
	lastTripped      time.Time
	probeInFlight    bool
	probeStarted     time.Time
}

// NewCircuitBreakerRouter constructs a 3-state circuit breaker router.
func NewCircuitBreakerRouter(cfg CircuitBreakerConfig) *CircuitBreakerRouter {
	failThresh := cfg.FailureThreshold
	if failThresh <= 0 {
		failThresh = 3
	}
	winThresh := cfg.SuccessThreshold
	if winThresh <= 0 {
		winThresh = 2
	}
	cd := cfg.Cooldown
	if cd <= 0 {
		cd = 30 * time.Second
	}

	return &CircuitBreakerRouter{
		primary:          cfg.Primary,
		fallback:         cfg.Fallback,
		failureThreshold: failThresh,
		successThreshold: winThresh,
		cooldown:         cd,
		state:            CircuitClosed,
	}
}

// State returns the current circuit breaker status.
func (r *CircuitBreakerRouter) State() CircuitState {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == CircuitOpen && time.Since(r.lastTripped) > r.cooldown {
		r.state = CircuitHalfOpen
		r.consecutiveWins = 0
	}
	return r.state
}

// Select resolves either the primary or fallback driver based on circuit health.
func (r *CircuitBreakerRouter) Select(ctx context.Context, rc RouteContext) (blobkit.Driver, error) {
	if rc.ForcedProvider != "" {
		if r.primary != nil && rc.ForcedProvider == r.primary.Name() {
			return r.primary, nil
		}
		if r.fallback != nil && rc.ForcedProvider == r.fallback.Name() {
			return r.fallback, nil
		}
		return nil, fmt.Errorf("%w: driver %q not found in circuit breaker", blobkit.ErrProviderUnavailable, rc.ForcedProvider)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if open circuit can enter half-open for canary probe
	if r.state == CircuitOpen {
		if time.Since(r.lastTripped) > r.cooldown {
			r.state = CircuitHalfOpen
			r.consecutiveWins = 0
			r.probeInFlight = true
			r.probeStarted = time.Now()
			return r.primary, nil // Grant canary probe lease
		}

		if r.fallback != nil {
			return r.fallback, nil
		}
		return nil, fmt.Errorf("%w: primary driver circuit is open", blobkit.ErrProviderUnavailable)
	}

	if r.state == CircuitHalfOpen {
		if !r.probeInFlight || time.Since(r.probeStarted) > r.cooldown {
			r.probeInFlight = true
			r.probeStarted = time.Now()
			return r.primary, nil // Grant canary probe lease
		}
		// Probe already in-flight: route other traffic to fallback
		if r.fallback != nil {
			return r.fallback, nil
		}
		return nil, fmt.Errorf("%w: primary driver circuit is half-open (probe in flight)", blobkit.ErrProviderUnavailable)
	}

	return r.primary, nil
}

// AllDrivers returns all drivers managed by this circuit breaker router.
func (r *CircuitBreakerRouter) AllDrivers() []blobkit.Driver {
	var list []blobkit.Driver
	if r.primary != nil {
		list = append(list, r.primary)
	}
	if r.fallback != nil {
		list = append(list, r.fallback)
	}
	return list
}

// ReportFailure updates the breaker on execution failure for transient errors.
// Permanent client errors (e.g. 404, bad keys) do not trip the circuit breaker.
func (r *CircuitBreakerRouter) ReportFailure(driver string, err error) {
	if r.primary == nil || driver != r.primary.Name() {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == CircuitHalfOpen {
		r.probeInFlight = false
		if err != nil && blobkit.IsPermanent(err) {
			// Permanent client errors do not trip breaker back to Open, but release the probe lease
			return
		}
		// Immediate trip back to open on canary failure
		r.state = CircuitOpen
		r.lastTripped = time.Now()
		r.consecutiveWins = 0
		return
	}

	if err != nil && blobkit.IsPermanent(err) {
		return
	}

	if r.state == CircuitClosed {
		r.consecutiveFails++
		if r.consecutiveFails >= r.failureThreshold {
			r.state = CircuitOpen
			r.lastTripped = time.Now()
		}
	}
}

// ReportSuccess updates the breaker on successful execution.
func (r *CircuitBreakerRouter) ReportSuccess(driver string) {
	if r.primary == nil || driver != r.primary.Name() {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == CircuitHalfOpen {
		r.probeInFlight = false
		r.consecutiveWins++
		if r.consecutiveWins >= r.successThreshold {
			r.state = CircuitClosed
			r.consecutiveFails = 0
			r.consecutiveWins = 0
		}
		return
	}

	if r.state == CircuitClosed {
		r.consecutiveFails = 0
	}
}
