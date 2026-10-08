package router

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/suhwr/blobkit"
)

// NamespaceRouter routes storage operations to specific providers based on the object's namespace prefix.
type NamespaceRouter struct {
	mu            sync.RWMutex
	routes        map[string]blobkit.Driver // namespace prefix -> driver
	defaultDriver blobkit.Driver
	allDrivers    map[string]blobkit.Driver
}

// NewNamespaceRouter creates a router with a fallback default driver.
func NewNamespaceRouter(defaultDriver blobkit.Driver) *NamespaceRouter {
	all := make(map[string]blobkit.Driver)
	if defaultDriver != nil {
		all[defaultDriver.Name()] = defaultDriver
	}
	return &NamespaceRouter{
		routes:        make(map[string]blobkit.Driver),
		defaultDriver: defaultDriver,
		allDrivers:    all,
	}
}

// Register binds a namespace prefix to a specific provider driver.
func (r *NamespaceRouter) Register(namespacePrefix string, driver blobkit.Driver) {
	if driver == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	clean := strings.Trim(namespacePrefix, "/")
	r.routes[clean] = driver
	r.allDrivers[driver.Name()] = driver
}

// Select determines the driver by matching the longest prefix against the namespace.
func (r *NamespaceRouter) Select(ctx context.Context, rc RouteContext) (blobkit.Driver, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if rc.ForcedProvider != "" {
		d, ok := r.allDrivers[rc.ForcedProvider]
		if !ok {
			return nil, fmt.Errorf("%w: driver %q not registered in namespace router", blobkit.ErrProviderUnavailable, rc.ForcedProvider)
		}
		return d, nil
	}

	target := strings.Trim(rc.Namespace, "/")
	cleanKey := strings.Trim(rc.Key, "/")

	// Exact or longest prefix match
	var bestMatch string
	var selected blobkit.Driver

	for prefix, driver := range r.routes {
		matched := false
		if target != "" && (target == prefix || strings.HasPrefix(target, prefix+"/")) {
			matched = true
		}
		if !matched && cleanKey != "" && (cleanKey == prefix || strings.HasPrefix(cleanKey, prefix+"/")) {
			matched = true
		}
		if matched {
			if len(prefix) > len(bestMatch) {
				bestMatch = prefix
				selected = driver
			}
		}
	}

	if selected != nil {
		return selected, nil
	}

	if r.defaultDriver != nil {
		return r.defaultDriver, nil
	}

	return nil, blobkit.ErrProviderUnavailable
}

// AllDrivers returns all distinct registered drivers.
func (r *NamespaceRouter) AllDrivers() []blobkit.Driver {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]blobkit.Driver, 0, len(r.allDrivers))
	for _, d := range r.allDrivers {
		list = append(list, d)
	}
	return list
}

func (r *NamespaceRouter) ReportFailure(driverName string, err error) {}
func (r *NamespaceRouter) ReportSuccess(driverName string)            {}
