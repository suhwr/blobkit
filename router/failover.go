package router

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/suhwr/blobkit"
)

// FailoverConfig configures active-passive failover between two drivers.
type FailoverConfig struct {
	Primary                blobkit.Driver
	Secondary              blobkit.Driver
	MaxConsecutiveFailures int
	Cooldown               time.Duration
}

// FailoverRouter provides automatic failover from a primary driver to a secondary driver
// when consecutive 5xx or network errors occur, with cooldown-based probing recovery.
type FailoverRouter struct {
	primary   blobkit.Driver
	secondary blobkit.Driver

	maxFailures int
	cooldown    time.Duration

	mu          sync.RWMutex
	failures    int
	lastTripped time.Time
}

// NewFailoverRouter creates a FailoverRouter with the given configuration.
func NewFailoverRouter(cfg FailoverConfig) *FailoverRouter {
	maxF := cfg.MaxConsecutiveFailures
	if maxF <= 0 {
		maxF = 3
	}
	cd := cfg.Cooldown
	if cd <= 0 {
		cd = 30 * time.Second
	}
	return &FailoverRouter{
		primary:     cfg.Primary,
		secondary:   cfg.Secondary,
		maxFailures: maxF,
		cooldown:    cd,
	}
}

// Select chooses either the primary or secondary driver based on primary circuit health.
func (r *FailoverRouter) Select(ctx context.Context, rc RouteContext) (blobkit.Driver, error) {
	if rc.ForcedProvider != "" {
		if r.primary != nil && rc.ForcedProvider == r.primary.Name() {
			return r.primary, nil
		}
		if r.secondary != nil && rc.ForcedProvider == r.secondary.Name() {
			return r.secondary, nil
		}
		return nil, fmt.Errorf("%w: requested provider %q not found in failover router", blobkit.ErrProviderUnavailable, rc.ForcedProvider)
	}

	r.mu.RLock()
	isTripped := r.failures >= r.maxFailures
	trippedTime := r.lastTripped
	r.mu.RUnlock()

	if !isTripped {
		if r.primary != nil {
			return r.primary, nil
		}
		return r.secondary, nil
	}

	// In cooldown: check if cooldown duration has expired for a canary probe
	if time.Since(trippedTime) >= r.cooldown {
		// Allow probing primary
		return r.primary, nil
	}

	// Route to secondary
	if r.secondary != nil {
		return r.secondary, nil
	}
	return r.primary, nil
}

// AllDrivers returns both primary and secondary drivers.
func (r *FailoverRouter) AllDrivers() []blobkit.Driver {
	var list []blobkit.Driver
	if r.primary != nil {
		list = append(list, r.primary)
	}
	if r.secondary != nil {
		list = append(list, r.secondary)
	}
	return list
}

// ReportFailure records a failure against the driver.
func (r *FailoverRouter) ReportFailure(driverName string, err error) {
	if r.primary != nil && driverName == r.primary.Name() {
		r.mu.Lock()
		r.failures++
		if r.failures >= r.maxFailures {
			r.lastTripped = time.Now()
		}
		r.mu.Unlock()
	}
}

// ReportSuccess resets failure counters upon successful execution.
func (r *FailoverRouter) ReportSuccess(driverName string) {
	if r.primary != nil && driverName == r.primary.Name() {
		r.mu.Lock()
		r.failures = 0
		r.mu.Unlock()
	}
}
