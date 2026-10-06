package blobkit

import "context"

// Operation represents the high-level storage action being performed.
type Operation string

const (
	OpPut             Operation = "put"
	OpGet             Operation = "get"
	OpHead            Operation = "head"
	OpDelete          Operation = "delete"
	OpList            Operation = "list"
	OpPresign         Operation = "presign"
	OpCopy            Operation = "copy"
	OpMove            Operation = "move"
	OpUploadPart      Operation = "upload_part"
	OpMultipart       Operation = "multipart"
	OpSoftDelete      Operation = "soft_delete"
	OpRestore         Operation = "restore"
	OpPermanentDelete Operation = "permanent_delete"
)

// RouteContext carries routing intent from the application or BlobKit core to the router.
type RouteContext struct {
	// Op is the current operation.
	Op Operation

	// Key is the target physical storage key (if known).
	Key string

	// Namespace is the semantic category/partition (e.g. "avatars", "backups").
	Namespace string

	// ForcedProvider is an optional explicit provider name requested by the caller.
	ForcedProvider string
}

// Router determines which storage provider driver should execute an operation.
type Router interface {
	// Select resolves the appropriate driver for the given operation context.
	Select(ctx context.Context, rc RouteContext) (Driver, error)

	// AllDrivers returns all registered drivers under this router.
	AllDrivers() []Driver

	// ReportFailure notifies the router of an execution failure for health tracking.
	ReportFailure(driverName string, err error)

	// ReportSuccess notifies the router of a successful execution.
	ReportSuccess(driverName string)
}

// defaultFixedRouter is an internal fallback router directing all traffic to a single driver.
type defaultFixedRouter struct {
	driver Driver
}

func (r *defaultFixedRouter) Select(ctx context.Context, rc RouteContext) (Driver, error) {
	if r.driver == nil {
		return nil, ErrProviderUnavailable
	}
	if rc.ForcedProvider != "" && rc.ForcedProvider != r.driver.Name() {
		return nil, ErrProviderUnavailable
	}
	return r.driver, nil
}

func (r *defaultFixedRouter) AllDrivers() []Driver {
	if r.driver == nil {
		return nil
	}
	return []Driver{r.driver}
}

func (r *defaultFixedRouter) ReportFailure(string, error) {}
func (r *defaultFixedRouter) ReportSuccess(string)        {}
