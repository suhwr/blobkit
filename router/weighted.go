package router

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/suhwr/blobkit"
)

// WeightedTarget associates a storage driver with an integer traffic weight.
type WeightedTarget struct {
	Driver blobkit.Driver
	Weight int
}

// WeightedRouter distributes operations across multiple storage backends
// proportional to their assigned weights.
type WeightedRouter struct {
	mu          sync.RWMutex
	targets     []WeightedTarget
	totalWeight int
	rng         *rand.Rand
}

// NewWeightedRouter constructs a WeightedRouter with the provided targets.
func NewWeightedRouter(targets []WeightedTarget) (*WeightedRouter, error) {
	if len(targets) == 0 {
		return nil, errors.New("weighted router requires at least one target")
	}

	total := 0
	validTargets := make([]WeightedTarget, 0, len(targets))
	for _, t := range targets {
		if t.Driver == nil {
			return nil, errors.New("target driver cannot be nil")
		}
		if t.Weight <= 0 {
			continue
		}
		total += t.Weight
		validTargets = append(validTargets, t)
	}

	if total == 0 {
		return nil, errors.New("total weight must be greater than zero")
	}

	return &WeightedRouter{
		targets:     validTargets,
		totalWeight: total,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Select resolves a driver according to weight distribution or ForcedProvider override.
func (r *WeightedRouter) Select(ctx context.Context, rc RouteContext) (blobkit.Driver, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. Check ForcedProvider override
	if rc.ForcedProvider != "" {
		for _, t := range r.targets {
			if t.Driver.Name() == rc.ForcedProvider {
				return t.Driver, nil
			}
		}
		return nil, fmt.Errorf("%w: driver %q not found", blobkit.ErrProviderUnavailable, rc.ForcedProvider)
	}

	// 2. Proportional random selection
	val := r.rng.Intn(r.totalWeight)
	accum := 0
	for _, t := range r.targets {
		accum += t.Weight
		if val < accum {
			return t.Driver, nil
		}
	}

	return r.targets[len(r.targets)-1].Driver, nil
}

// AllDrivers returns all registered drivers.
func (r *WeightedRouter) AllDrivers() []blobkit.Driver {
	r.mu.RLock()
	defer r.mu.RUnlock()

	drivers := make([]blobkit.Driver, len(r.targets))
	for i, t := range r.targets {
		drivers[i] = t.Driver
	}
	return drivers
}

// ReportFailure satisfies the Router interface.
func (r *WeightedRouter) ReportFailure(driver string, err error) {}

// ReportSuccess satisfies the Router interface.
func (r *WeightedRouter) ReportSuccess(driver string) {}
