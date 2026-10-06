package router

import (
	"context"
	"fmt"

	"github.com/suhwr/blobkit"
)

// FixedRouter directs all traffic to a single designated provider driver.
type FixedRouter struct {
	driver blobkit.Driver
}

// NewFixedRouter constructs a FixedRouter with the provided driver.
func NewFixedRouter(driver blobkit.Driver) *FixedRouter {
	return &FixedRouter{driver: driver}
}

// Select returns the configured driver unless an incompatible provider is explicitly forced.
func (r *FixedRouter) Select(ctx context.Context, rc RouteContext) (blobkit.Driver, error) {
	if r.driver == nil {
		return nil, blobkit.ErrProviderUnavailable
	}
	if rc.ForcedProvider != "" && rc.ForcedProvider != r.driver.Name() {
		return nil, fmt.Errorf("%w: driver %q is not registered in fixed router", blobkit.ErrProviderUnavailable, rc.ForcedProvider)
	}
	return r.driver, nil
}

// AllDrivers returns a slice containing the single configured driver.
func (r *FixedRouter) AllDrivers() []blobkit.Driver {
	if r.driver == nil {
		return nil
	}
	return []blobkit.Driver{r.driver}
}

func (r *FixedRouter) ReportFailure(driverName string, err error) {}
func (r *FixedRouter) ReportSuccess(driverName string)            {}
