package router

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/suhwr/blobkit"
)

// TierTarget represents a storage provider account within a priority tier.
type TierTarget struct {
	Driver      blobkit.Driver
	Weight      int           // Traffic distribution weight within this tier (default: 1)
	MaxFailures int           // Consecutive failures before entering cooldown (default: 3)
	Cooldown    time.Duration // Duration to back off when circuit is tripped (default: 30s)
}

// targetState tracks real-time health and cooldown for an individual driver target.
type targetState struct {
	target      TierTarget
	failures    int
	lastTripped time.Time
}

// Tier defines a priority level containing one or more storage accounts.
type Tier struct {
	Name    string
	Targets []TierTarget
}

// TieredRouterConfig configures a multi-tier cascading router.
type TieredRouterConfig struct {
	Tiers []Tier
}

// TieredRouter provides hierarchical, priority-based storage routing across multiple
// provider accounts (e.g., Tier 1: 10x R2 accounts, Tier 2: 10x Wasabi, Tier 3: 10x S3)
// with per-target circuit breakers, automatic cooldown on 429/5xx, and graceful cascading.
type TieredRouter struct {
	mu           sync.RWMutex
	tiers        []tierInternal
	driverLookup map[string]*targetState
	allDrivers   []blobkit.Driver
}

type tierInternal struct {
	name    string
	targets []*targetState
}

// NewTieredRouter initializes a TieredRouter with configured priority tiers.
func NewTieredRouter(cfg TieredRouterConfig) (*TieredRouter, error) {
	if len(cfg.Tiers) == 0 {
		return nil, errors.New("tiered router requires at least one tier")
	}

	lookup := make(map[string]*targetState)
	var allDrivers []blobkit.Driver
	var internalTiers []tierInternal

	for tierIdx, tier := range cfg.Tiers {
		if len(tier.Targets) == 0 {
			return nil, fmt.Errorf("tier %d (%q) has no targets", tierIdx, tier.Name)
		}

		var states []*targetState
		for _, t := range tier.Targets {
			if t.Driver == nil {
				return nil, fmt.Errorf("tier %q contains a nil driver", tier.Name)
			}
			if t.Weight <= 0 {
				t.Weight = 1
			}
			if t.MaxFailures <= 0 {
				t.MaxFailures = 3
			}
			if t.Cooldown <= 0 {
				t.Cooldown = 30 * time.Second
			}

			driverName := t.Driver.Name()
			if _, exists := lookup[driverName]; exists {
				return nil, fmt.Errorf("duplicate driver name %q registered in tiered router", driverName)
			}

			st := &targetState{
				target: t,
			}
			states = append(states, st)
			lookup[driverName] = st
			allDrivers = append(allDrivers, t.Driver)
		}

		internalTiers = append(internalTiers, tierInternal{
			name:    tier.Name,
			targets: states,
		})
	}

	return &TieredRouter{
		tiers:        internalTiers,
		driverLookup: lookup,
		allDrivers:   allDrivers,
	}, nil
}

// Select resolves an available driver by inspecting tiers in order of priority.
// Within each tier, it balances traffic among healthy targets and cascades down
// if all targets in a tier are currently tripped or rate-limited.
func (r *TieredRouter) Select(ctx context.Context, rc RouteContext) (blobkit.Driver, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 1. Direct provider lookup (e.g. read operations pinned to a specific account)
	if rc.ForcedProvider != "" {
		st, ok := r.driverLookup[rc.ForcedProvider]
		if !ok {
			return nil, fmt.Errorf("%w: driver %q not found in tiered router", blobkit.ErrProviderUnavailable, rc.ForcedProvider)
		}
		return st.target.Driver, nil
	}

	now := time.Now()

	// 2. Cascade through tiers in priority order
	for _, tier := range r.tiers {
		var healthy []*targetState
		totalWeight := 0

		for _, st := range tier.targets {
			isTripped := st.failures >= st.target.MaxFailures
			if isTripped {
				// Check if cooldown has expired for a canary probe
				if now.Sub(st.lastTripped) >= st.target.Cooldown {
					healthy = append(healthy, st)
					totalWeight += st.target.Weight
				}
			} else {
				healthy = append(healthy, st)
				totalWeight += st.target.Weight
			}
		}

		// If this tier has at least one healthy/probing target, select from it
		if len(healthy) > 0 {
			if len(healthy) == 1 || totalWeight <= 0 {
				return healthy[0].target.Driver, nil
			}

			// Proportional weighted selection using math/rand/v2
			val := rand.IntN(totalWeight)
			accum := 0
			for _, st := range healthy {
				accum += st.target.Weight
				if val < accum {
					return st.target.Driver, nil
				}
			}
			return healthy[len(healthy)-1].target.Driver, nil
		}

		// All targets in this tier are tripped; gracefully cascade to next tier
	}

	return nil, fmt.Errorf("%w: all tiers and storage providers are currently unavailable or in cooldown", blobkit.ErrProviderUnavailable)
}

// AllDrivers returns all drivers across all tiers.
func (r *TieredRouter) AllDrivers() []blobkit.Driver {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]blobkit.Driver(nil), r.allDrivers...)
}

// ReportFailure records a failure for a specific driver and trips its circuit if threshold is reached.
func (r *TieredRouter) ReportFailure(driverName string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	st, ok := r.driverLookup[driverName]
	if !ok {
		return
	}

	st.failures++
	if st.failures >= st.target.MaxFailures {
		st.lastTripped = time.Now()
	}
}

// ReportSuccess resets the failure counter for a specific driver.
func (r *TieredRouter) ReportSuccess(driverName string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	st, ok := r.driverLookup[driverName]
	if !ok {
		return
	}

	st.failures = 0
}
