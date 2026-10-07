package blobkit

import (
	"net/url"
	"strings"
)

// MaxKeyLength defines the maximum permitted byte length for an object key (1024 bytes, conforming to cloud standards).
const MaxKeyLength = 1024

// ValidateKey validates an object key against directory traversal, path escape, injection attacks,
// and malformed formatting across all operations and storage engines.
func ValidateKey(key string) error {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return ErrInvalidKey
	}
	if len(key) > MaxKeyLength {
		return ErrInvalidKey
	}

	// Reject null bytes, carriage returns, and newlines
	if strings.ContainsAny(key, "\x00\r\n") {
		return ErrSecurityViolation
	}

	// Reject root escapes / absolute path indicators
	if strings.HasPrefix(key, "/") || strings.HasPrefix(key, "\\") {
		return ErrSecurityViolation
	}

	// Check raw path segments
	if err := checkPathSegments(key); err != nil {
		return err
	}

	// Check URL-decoded path segments to prevent encoded traversal escapes (e.g. %2e%2e, %2e%2f)
	if strings.Contains(key, "%") {
		unescaped, err := url.PathUnescape(key)
		if err == nil && unescaped != key {
			if strings.ContainsAny(unescaped, "\x00\r\n") || strings.HasPrefix(unescaped, "/") || strings.HasPrefix(unescaped, "\\") {
				return ErrSecurityViolation
			}
			if err := checkPathSegments(unescaped); err != nil {
				return err
			}
		}
	}

	return nil
}

func checkPathSegments(k string) error {
	// Normalize backslashes to slashes to catch Windows-style path traversals
	normalized := strings.ReplaceAll(k, "\\", "/")
	segs := strings.Split(normalized, "/")
	for _, seg := range segs {
		if seg == ".." || seg == "." {
			return ErrSecurityViolation
		}
	}
	return nil
}
