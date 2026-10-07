package blobkit

import (
	"strings"
	"time"
)

// CheckPreconditions validates HTTP conditional request headers against object state
// according to RFC 7232 specifications, returning ErrPreconditionFailed if any condition is unmet.
func CheckPreconditions(etag string, modTime time.Time, opts GetOptions) error {
	trimmedETag := strings.TrimSpace(etag)
	isWeak := strings.HasPrefix(trimmedETag, "W/")
	cleanETag := strings.Trim(strings.TrimPrefix(trimmedETag, "W/"), "\"")

	// 1. IfMatch: object must strongly match one of the provided ETags
	// Weak entity tags MUST NOT match under strong comparison rules (RFC 7232 Section 2.3.2 & 3.1).
	if opts.IfMatch != "" {
		if opts.IfMatch == "*" {
			// Wildcard matches any existing representation
			if cleanETag == "" && modTime.IsZero() {
				return ErrPreconditionFailed
			}
		} else {
			if isWeak {
				// Weak entity tag cannot satisfy strong If-Match
				return ErrPreconditionFailed
			}
			matched := false
			for _, match := range strings.Split(opts.IfMatch, ",") {
				m := strings.TrimSpace(match)
				if strings.HasPrefix(m, "W/") {
					// Weak tag in If-Match cannot strongly match
					continue
				}
				if strings.Trim(m, "\"") == cleanETag {
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
	// Uses weak comparison (RFC 7232 Section 3.2): weak and strong representations with same opaque tag match.
	if opts.IfNoneMatch != "" {
		if opts.IfNoneMatch == "*" {
			return ErrPreconditionFailed
		}
		for _, match := range strings.Split(opts.IfNoneMatch, ",") {
			m := strings.TrimSpace(match)
			cleanMatch := strings.Trim(strings.TrimPrefix(m, "W/"), "\"")
			if cleanMatch == cleanETag {
				return ErrPreconditionFailed
			}
		}
	}

	// 3. IfModifiedSince: object must have been modified after the given timestamp
	// Note: RFC 7232 states If-Modified-Since is evaluated only if If-None-Match is absent.
	if opts.IfNoneMatch == "" && opts.IfModifiedSince != nil && !modTime.IsZero() {
		// HTTP timestamps have 1-second precision
		objSec := modTime.UTC().Truncate(time.Second)
		condSec := opts.IfModifiedSince.UTC().Truncate(time.Second)
		if !objSec.After(condSec) {
			return ErrPreconditionFailed
		}
	}

	// 4. IfUnmodifiedSince: object must NOT have been modified after the given timestamp
	// Note: RFC 7232 states If-Unmodified-Since is evaluated only if If-Match is absent.
	if opts.IfMatch == "" && opts.IfUnmodifiedSince != nil && !modTime.IsZero() {
		objSec := modTime.UTC().Truncate(time.Second)
		condSec := opts.IfUnmodifiedSince.UTC().Truncate(time.Second)
		if objSec.After(condSec) {
			return ErrPreconditionFailed
		}
	}

	return nil
}
