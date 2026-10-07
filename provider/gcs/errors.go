package gcs

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"github.com/suhwr/blobkit"
)

var (
	bearerTokenRegex    = regexp.MustCompile(`(?i)(Bearer\s+)[A-Za-z0-9\-\._~+/]+=*`)
	googSignatureRegex  = regexp.MustCompile(`(?i)(X-Goog-Signature=)[a-f0-9]+`)
	genericSigRegex     = regexp.MustCompile(`(?i)(sig=)[^&\s"']+`)
	privateKeyPemRegex  = regexp.MustCompile(`(?s)-----BEGIN[^\-]+PRIVATE KEY-----.*?-----END[^\-]+PRIVATE KEY-----`)
)

// gcsErrorResponse represents the standard error format returned by Google Cloud Storage JSON APIs.
type gcsErrorResponse struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Errors  []struct {
			Domain  string `json:"domain"`
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"errors"`
	} `json:"error"`
}

// scrubCredentials redacts sensitive tokens, signatures, and private keys from error messages.
func scrubCredentials(msg string) string {
	if msg == "" {
		return ""
	}
	s := bearerTokenRegex.ReplaceAllString(msg, "$1[REDACTED]")
	s = googSignatureRegex.ReplaceAllString(s, "$1[REDACTED]")
	s = genericSigRegex.ReplaceAllString(s, "$1[REDACTED]")
	s = privateKeyPemRegex.ReplaceAllString(s, "[REDACTED PRIVATE KEY]")
	return s
}

// parseGCSError parses the HTTP status code and response body from GCS and maps it to a canonical blobkit error.
func parseGCSError(op, key, driverName string, statusCode int, body []byte, rawErr error) error {
	if rawErr != nil {
		scrubbed := scrubCredentials(rawErr.Error())
		return blobkit.WrapError(op, key, driverName, errors.New(scrubbed))
	}

	var parsed gcsErrorResponse
	var errMsg string
	var reason string

	if len(body) > 0 {
		if err := json.Unmarshal(body, &parsed); err == nil && parsed.Error.Message != "" {
			errMsg = parsed.Error.Message
			if len(parsed.Error.Errors) > 0 {
				reason = parsed.Error.Errors[0].Reason
			}
		} else {
			errMsg = string(body)
		}
	}

	if errMsg == "" {
		errMsg = http.StatusText(statusCode)
	}
	errMsg = scrubCredentials(errMsg)

	switch {
	case statusCode == http.StatusNotFound || reason == "notFound":
		return blobkit.WrapError(op, key, driverName, blobkit.ErrObjectNotFound)

	case statusCode == http.StatusPreconditionFailed || statusCode == http.StatusRequestedRangeNotSatisfiable || reason == "conditionNotMet":
		return blobkit.WrapError(op, key, driverName, blobkit.ErrPreconditionFailed)

	case statusCode == http.StatusConflict:
		return blobkit.WrapError(op, key, driverName, blobkit.ErrObjectLocked)

	case statusCode == http.StatusTooManyRequests || statusCode >= 500 || reason == "rateLimitExceeded":
		return blobkit.WrapError(op, key, driverName, fmt.Errorf("%w: %s (status %d)", blobkit.ErrProviderUnavailable, errMsg, statusCode))

	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		return blobkit.WrapError(op, key, driverName, fmt.Errorf("authentication error: %s (status %d)", errMsg, statusCode))

	default:
		return blobkit.WrapError(op, key, driverName, fmt.Errorf("gcs api error: %s (status %d)", errMsg, statusCode))
	}
}
