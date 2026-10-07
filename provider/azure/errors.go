package azure

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/suhwr/blobkit"
)

var (
	sharedKeyAuthRegex = regexp.MustCompile(`(?i)SharedKey\s+[A-Za-z0-9_\-\.]+:[A-Za-z0-9+/=]+`)
	sasSigRegex        = regexp.MustCompile(`(?i)(sig=[A-Za-z0-9%+/=]+)`)
)

// azureXMLError matches the XML error payload returned by Azure Storage REST API.
type azureXMLError struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// scrubCredentials redacts sensitive access keys, signatures, and SAS tokens.
func scrubCredentials(msg string) string {
	msg = sharedKeyAuthRegex.ReplaceAllString(msg, "SharedKey [REDACTED]")
	msg = sasSigRegex.ReplaceAllString(msg, "sig=[REDACTED]")
	return msg
}

// wrapHTTPError translates Azure HTTP status codes and error XML payloads into BlobKit domain sentinel errors.
func wrapHTTPError(op, key, driverName string, statusCode int, body []byte, rawErr error) error {
	if statusCode == 0 && rawErr != nil {
		if blobkit.PreserveSentinel(rawErr) {
			return blobkit.WrapError(op, key, driverName, rawErr)
		}
		return blobkit.WrapError(op, key, driverName, errors.New(scrubCredentials(rawErr.Error())))
	}

	var xmlErr azureXMLError
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &xmlErr)
	}

	code := xmlErr.Code
	msg := strings.TrimSpace(xmlErr.Message)
	if msg == "" {
		msg = http.StatusText(statusCode)
	}
	msg = scrubCredentials(msg)

	switch {
	case code == "BlobNotFound" || code == "ResourceNotFound" || statusCode == http.StatusNotFound:
		return blobkit.WrapError(op, key, driverName, blobkit.ErrObjectNotFound)

	case code == "ContainerNotFound" || code == "ContainerBeingDeleted":
		return blobkit.WrapError(op, key, driverName, blobkit.ErrBucketNotFound)

	case code == "ConditionNotMet" || code == "TargetConditionNotMet" || statusCode == http.StatusPreconditionFailed || statusCode == http.StatusNotModified || statusCode == http.StatusRequestedRangeNotSatisfiable:
		return blobkit.WrapError(op, key, driverName, blobkit.ErrPreconditionFailed)

	case code == "ServerBusy" || statusCode == http.StatusTooManyRequests || statusCode >= 500:
		return blobkit.WrapError(op, key, driverName, blobkit.ErrProviderUnavailable)

	case statusCode == http.StatusForbidden || statusCode == http.StatusUnauthorized:
		return blobkit.WrapError(op, key, driverName, fmt.Errorf("azure auth error (%d): %s", statusCode, msg))

	default:
		if statusCode >= 400 && statusCode < 500 {
			return blobkit.WrapError(op, key, driverName, fmt.Errorf("azure client error (%d - %s): %s", statusCode, code, msg))
		} else if statusCode >= 500 {
			return blobkit.WrapError(op, key, driverName, blobkit.ErrProviderUnavailable)
		}
	}

	if rawErr != nil {
		return blobkit.WrapError(op, key, driverName, errors.New(scrubCredentials(rawErr.Error())))
	}
	return blobkit.WrapError(op, key, driverName, fmt.Errorf("unexpected status %d: %s", statusCode, msg))
}
