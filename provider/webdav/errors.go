package webdav

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"github.com/suhwr/blobkit"
)

var (
	basicAuthRegex  = regexp.MustCompile(`(?i)Basic\s+[A-Za-z0-9+/=]+`)
	bearerAuthRegex = regexp.MustCompile(`(?i)Bearer\s+[A-Za-z0-9_\-\.]+`)
)

// scrubCredentials removes sensitive auth tokens and basic auth strings from error messages.
func scrubCredentials(msg string) string {
	msg = basicAuthRegex.ReplaceAllString(msg, "Basic [REDACTED]")
	msg = bearerAuthRegex.ReplaceAllString(msg, "Bearer [REDACTED]")
	return msg
}

// wrapHTTPError translates WebDAV HTTP response status codes into BlobKit domain sentinel errors.
func wrapHTTPError(op, key, driverName string, statusCode int, rawErr error) error {
	if statusCode == 0 && rawErr != nil {
		if blobkit.PreserveSentinel(rawErr) {
			return blobkit.WrapError(op, key, driverName, rawErr)
		}
		return blobkit.WrapError(op, key, driverName, errors.New(scrubCredentials(rawErr.Error())))
	}

	statusText := http.StatusText(statusCode)
	if statusText == "" {
		statusText = fmt.Sprintf("status %d", statusCode)
	}

	switch statusCode {
	case http.StatusNotFound:
		return blobkit.WrapError(op, key, driverName, blobkit.ErrObjectNotFound)

	case http.StatusPreconditionFailed, http.StatusRequestedRangeNotSatisfiable, http.StatusNotModified:
		return blobkit.WrapError(op, key, driverName, blobkit.ErrPreconditionFailed)

	case http.StatusTooManyRequests:
		return blobkit.WrapError(op, key, driverName, blobkit.ErrProviderUnavailable)

	case http.StatusUnauthorized:
		return blobkit.WrapError(op, key, driverName, fmt.Errorf("webdav unauthorized: invalid username or password"))

	case http.StatusForbidden:
		return blobkit.WrapError(op, key, driverName, fmt.Errorf("webdav forbidden: access denied"))

	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return blobkit.WrapError(op, key, driverName, blobkit.ErrProviderUnavailable)

	default:
		if statusCode >= 400 && statusCode < 500 {
			return blobkit.WrapError(op, key, driverName, fmt.Errorf("webdav client error (%d): %s", statusCode, statusText))
		} else if statusCode >= 500 {
			return blobkit.WrapError(op, key, driverName, blobkit.ErrProviderUnavailable)
		}
	}

	if rawErr != nil {
		return blobkit.WrapError(op, key, driverName, errors.New(scrubCredentials(rawErr.Error())))
	}
	return blobkit.WrapError(op, key, driverName, fmt.Errorf("unexpected http status %d: %s", statusCode, statusText))
}
