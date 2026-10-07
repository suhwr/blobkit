package router

import (
	"context"
	"fmt"

	"github.com/suhwr/blobkit"
)

// CapabilityRouter inspects driver capabilities and routes requests only to drivers
// that support required operations.
type CapabilityRouter struct {
	drivers []blobkit.Driver
}

// NewCapabilityRouter constructs a CapabilityRouter with the provided pool of storage drivers.
func NewCapabilityRouter(drivers ...blobkit.Driver) *CapabilityRouter {
	return &CapabilityRouter{drivers: drivers}
}

// Select resolves an available driver from the pool matching the requested operation.
func (r *CapabilityRouter) Select(ctx context.Context, rc RouteContext) (blobkit.Driver, error) {
	if rc.ForcedProvider != "" {
		for _, d := range r.drivers {
			if d.Name() == rc.ForcedProvider {
				return d, nil
			}
		}
		return nil, fmt.Errorf("%w: driver %q not found", blobkit.ErrProviderUnavailable, rc.ForcedProvider)
	}

	var required blobkit.Capability
	switch rc.Op {
	case blobkit.OpCopy:
		required = blobkit.CapCopy
	case blobkit.OpUploadPart, blobkit.OpMultipart:
		required = blobkit.CapMultipartSession
	case blobkit.OpPresign:
		required = blobkit.CapPresignGet
	default:
		required = blobkit.CapDirectPut
	}

	for _, d := range r.drivers {
		if d.Capabilities()&required == required {
			return d, nil
		}
	}

	if len(r.drivers) == 0 {
		return nil, fmt.Errorf("%w: no driver registered", blobkit.ErrProviderUnavailable)
	}

	return nil, fmt.Errorf("%w: operation %s requires capability %v", blobkit.ErrUnsupportedOperation, rc.Op, required)
}

// AllDrivers returns all registered drivers.
func (r *CapabilityRouter) AllDrivers() []blobkit.Driver {
	out := make([]blobkit.Driver, len(r.drivers))
	copy(out, r.drivers)
	return out
}

// ReportFailure satisfies the Router interface.
func (r *CapabilityRouter) ReportFailure(driver string, err error) {}

// ReportSuccess satisfies the Router interface.
func (r *CapabilityRouter) ReportSuccess(driver string) {}
