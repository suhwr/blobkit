package fleet

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/router"
)

// Fleet orchestrates a collection of heterogeneous storage providers and their routing topology.
type Fleet struct {
	mu      sync.RWMutex
	drivers map[string]blobkit.Driver
	order   []string
	router  blobkit.Router
	cfg     FleetConfig
}

// Load validates the declarative configuration, instantiates all defined drivers,
// and auto-wires the designated routing strategy.
func Load(cfg FleetConfig) (*Fleet, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	drivers := make(map[string]blobkit.Driver)
	var order []string

	// Instantiate drivers
	for _, p := range cfg.Providers {
		d, err := buildDriver(p)
		if err != nil {
			// Clean up already initialized drivers on partial failure
			for _, prev := range drivers {
				_ = prev.Close()
			}
			return nil, fmt.Errorf("fleet: failed to initialize provider %q: %w", p.Name, err)
		}
		drivers[p.Name] = d
		order = append(order, p.Name)
	}

	// Auto-wire routing topology
	var fleetRouter blobkit.Router
	switch cfg.Routing.Strategy {
	case StrategyFixed:
		fleetRouter = router.NewFixedRouter(drivers[cfg.Routing.DefaultProvider])

	case StrategyNamespace:
		var defaultDriver blobkit.Driver
		if cfg.Routing.DefaultProvider != "" {
			defaultDriver = drivers[cfg.Routing.DefaultProvider]
		}
		nsRouter := router.NewNamespaceRouter(defaultDriver)
		for ns, pName := range cfg.Routing.Namespaces {
			nsRouter.Register(ns, drivers[pName])
		}
		fleetRouter = nsRouter

	case StrategyFailover:
		fleetRouter = router.NewFailoverRouter(router.FailoverConfig{
			Primary:                drivers[cfg.Routing.Failover.Primary],
			Secondary:              drivers[cfg.Routing.Failover.Secondary],
			MaxConsecutiveFailures: cfg.Routing.Failover.MaxConsecutiveFailures,
			Cooldown:               cfg.Routing.Failover.Cooldown,
		})

	case StrategyCircuitBreaker:
		fleetRouter = router.NewCircuitBreakerRouter(router.CircuitBreakerConfig{
			Primary:          drivers[cfg.Routing.CircuitBreaker.Primary],
			Fallback:         drivers[cfg.Routing.CircuitBreaker.Fallback],
			FailureThreshold: cfg.Routing.CircuitBreaker.FailureThreshold,
			SuccessThreshold: cfg.Routing.CircuitBreaker.SuccessThreshold,
			Cooldown:         cfg.Routing.CircuitBreaker.Cooldown,
		})

	case StrategyWeighted:
		var targets []router.WeightedTarget
		for pName, w := range cfg.Routing.Weights {
			targets = append(targets, router.WeightedTarget{
				Driver: drivers[pName],
				Weight: w,
			})
		}
		wRouter, err := router.NewWeightedRouter(targets)
		if err != nil {
			for _, d := range drivers {
				_ = d.Close()
			}
			return nil, fmt.Errorf("fleet: failed to construct weighted router: %w", err)
		}
		fleetRouter = wRouter

	default:
		for _, d := range drivers {
			_ = d.Close()
		}
		return nil, fmt.Errorf("fleet: unsupported routing strategy %q", cfg.Routing.Strategy)
	}

	return &Fleet{
		drivers: drivers,
		order:   order,
		router:  fleetRouter,
		cfg:     cfg,
	}, nil
}

// LoadFromJSON deserializes a JSON payload and initializes the fleet.
func LoadFromJSON(data []byte) (*Fleet, error) {
	var cfg FleetConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("fleet: invalid json configuration: %w", err)
	}
	return Load(cfg)
}

// LoadFromFile reads a JSON configuration file from disk and initializes the fleet.
func LoadFromFile(filePath string) (*Fleet, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("fleet: failed to read configuration file: %w", err)
	}
	return LoadFromJSON(data)
}

// Driver returns an instantiated driver by its configured name.
func (f *Fleet) Driver(name string) (blobkit.Driver, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	d, ok := f.drivers[name]
	return d, ok
}

// Drivers returns a copy of all active drivers keyed by name.
func (f *Fleet) Drivers() map[string]blobkit.Driver {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]blobkit.Driver, len(f.drivers))
	for k, v := range f.drivers {
		out[k] = v
	}
	return out
}

// Router returns the fully wired composite or specialized router.
func (f *Fleet) Router() blobkit.Router {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.router
}

// NewClient constructs a high-level BlobKit Client pre-configured with the fleet's router.
func (f *Fleet) NewClient(opts ...blobkit.Option) (*blobkit.Client, error) {
	f.mu.RLock()
	r := f.router
	f.mu.RUnlock()

	allOpts := append([]blobkit.Option{blobkit.WithRouter(r)}, opts...)
	return blobkit.New(allOpts...)
}

// Close gracefully terminates all initialized fleet drivers in reverse instantiation order.
func (f *Fleet) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var errs []string
	for i := len(f.order) - 1; i >= 0; i-- {
		name := f.order[i]
		if d, ok := f.drivers[name]; ok {
			if err := d.Close(); err != nil {
				errs = append(errs, fmt.Sprintf("%s: %v", name, err))
			}
			delete(f.drivers, name)
		}
	}
	f.order = nil

	if len(errs) > 0 {
		return errors.New("fleet cleanup errors: " + strings.Join(errs, "; "))
	}
	return nil
}
