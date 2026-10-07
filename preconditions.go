package blobkit

import (
	"strings"
	"time"
)

// CheckPreconditions validates HTTP conditional request headers against object state,
// returning ErrPreconditionFailed if any condition is unmet.
func CheckPreconditions(etag string, modTime time.Time, opts GetOptions) error {
	cleanETag := strings.Trim(etag, "\"")

	// 1. IfMatch: object must match one of the provided ETags
	if opts.IfMatch != "" {
		if opts.IfMatch != "*" {
			matched := false
			for _, match := range strings.Split(opts.IfMatch, ",") {
				if strings.TrimSpace(strings.Trim(match, "\"")) == cleanETag {
					matched = true
					break
				}
			}
			if !matched {
				return ErrPreconditionFailed
			}
		}
	}

	// 2. IfNoneMatch: object must NOT match any of the provided ETags
	if opts.IfNoneMatch != "" {
		if opts.IfNoneMatch == "*" {
			return ErrPreconditionFailed
		}
		for _, match := range strings.Split(opts.IfNoneMatch, ",") {
			if strings.TrimSpace(strings.Trim(match, "\"")) == cleanETag {
				return ErrPreconditionFailed
			}
		}
	}

	// 3. IfModifiedSince: object must have been modified after the given timestamp
	if opts.IfModifiedSince != nil && !modTime.IsZero() {
		// HTTP timestamps have 1-second precision
		objSec := modTime.UTC().Truncate(time.Second)
		condSec := opts.IfModifiedSince.UTC().Truncate(time.Second)
		if !objSec.After(condSec) {
			return ErrPreconditionFailed
		}
	}

	// 4. IfUnmodifiedSince: object must NOT have been modified after the given timestamp
	if opts.IfUnmodifiedSince != nil && !modTime.IsZero() {
		objSec := modTime.UTC().Truncate(time.Second)
		condSec := opts.IfUnmodifiedSince.UTC().Truncate(time.Second)
		if objSec.After(condSec) {
			return ErrPreconditionFailed
		}
	}

	return nil
}
