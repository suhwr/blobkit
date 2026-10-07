package gdrive

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/suhwr/blobkit"
)

var (
	bearerTokenRegex = regexp.MustCompile(`(?i)Bearer\s+[A-Za-z0-9_\-\.]+`)
	uploadURIRegex   = regexp.MustCompile(`(?i)(upload_id=[A-Za-z0-9_\-]+)`)
)

// googleAPIErrorResponse mirrors the standard Google REST API error JSON structure.
type googleAPIErrorResponse struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Errors  []struct {
			Message string `json:"message"`
			Domain  string `json:"domain"`
			Reason  string `json:"reason"`
		} `json:"errors"`
	} `json:"error"`
}

// scrubMessage removes sensitive access tokens and session URLs from log/error strings.
func scrubMessage(msg string) string {
	msg = bearerTokenRegex.ReplaceAllString(msg, "Bearer [REDACTED]")
	msg = uploadURIRegex.ReplaceAllString(msg, "upload_id=[REDACTED]")
	return msg
}

// wrapHTTPError translates Google Drive HTTP response codes and error payloads into BlobKit domain sentinel errors.
func wrapHTTPError(op, key, driverName string, statusCode int, body []byte, rawErr error) error {
	if statusCode == 0 && rawErr != nil {
		if errors.Is(rawErr, blobkit.ErrObjectNotFound) ||
			errors.Is(rawErr, blobkit.ErrBucketNotFound) ||
			errors.Is(rawErr, blobkit.ErrQuotaExceeded) ||
			errors.Is(rawErr, blobkit.ErrProviderUnavailable) ||
			errors.Is(rawErr, blobkit.ErrPreconditionFailed) ||
			errors.Is(rawErr, blobkit.ErrUnsupportedOperation) {
			return blobkit.WrapError(op, key, driverName, rawErr)
		}
		return blobkit.WrapError(op, key, driverName, errors.New(scrubMessage(rawErr.Error())))
	}

	var apiErr googleAPIErrorResponse
	if len(body) > 0 {
		_ = json.Unmarshal(body, &apiErr)
	}

	errMsg := strings.TrimSpace(apiErr.Error.Message)
	if errMsg == "" && len(body) > 0 && len(body) < 512 {
		errMsg = strings.TrimSpace(string(body))
	}
	if errMsg == "" {
		errMsg = http.StatusText(statusCode)
	}
	errMsg = scrubMessage(errMsg)

	switch statusCode {
	case http.StatusNotFound:
		return blobkit.WrapError(op, key, driverName, blobkit.ErrObjectNotFound)

	case http.StatusPreconditionFailed, http.StatusRequestedRangeNotSatisfiable:
		return blobkit.WrapError(op, key, driverName, blobkit.ErrPreconditionFailed)

	case http.StatusTooManyRequests:
		return blobkit.WrapError(op, key, driverName, blobkit.ErrProviderUnavailable)

	case http.StatusForbidden:
		for _, e := range apiErr.Error.Errors {
			switch e.Reason {
			case "storageQuotaExceeded":
				return blobkit.WrapError(op, key, driverName, blobkit.ErrQuotaExceeded)
			case "rateLimitExceeded", "userRateLimitExceeded", "dailyLimitExceeded":
				return blobkit.WrapError(op, key, driverName, blobkit.ErrProviderUnavailable)
			}
		}
		return blobkit.WrapError(op, key, driverName, fmt.Errorf("forbidden: %s", errMsg))

	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return blobkit.WrapError(op, key, driverName, blobkit.ErrProviderUnavailable)

	default:
		if statusCode >= 400 && statusCode < 500 {
			return blobkit.WrapError(op, key, driverName, fmt.Errorf("client error (%d): %s", statusCode, errMsg))
		} else if statusCode >= 500 {
			return blobkit.WrapError(op, key, driverName, blobkit.ErrProviderUnavailable)
		}
	}

	if rawErr != nil {
		return blobkit.WrapError(op, key, driverName, errors.New(scrubMessage(rawErr.Error())))
	}
	return blobkit.WrapError(op, key, driverName, fmt.Errorf("unexpected http status %d: %s", statusCode, errMsg))
}
