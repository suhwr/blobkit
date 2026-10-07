package blobkit

import (
	"net/url"
	"strings"
)

// MaxKeyLength defines the maximum permitted byte length for an object key (1024 bytes, conforming to cloud standards).
const MaxKeyLength = 1024

// ValidateKey validates an object key against directory traversal, path escape, injection attacks,
// double/multi-encoding tricks, control characters, and malformed formatting across all operations and storage engines.
func ValidateKey(key string) error {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return ErrInvalidKey
	}
	if len(key) > MaxKeyLength {
		return ErrInvalidKey
	}

	// Reject ASCII control characters (0x00 - 0x1F, 0x7F) including null, CRLF, tabs
	for i := 0; i < len(key); i++ {
		b := key[i]
		if b < 0x20 || b == 0x7F {
			return ErrSecurityViolation
		}
	}

	// Reject Unicode bidi overrides, zero-width chars, and fullwidth dots/slashes
	for _, r := range key {
		switch r {
		case '\u200E', '\u200F', '\u202A', '\u202B', '\u202C', '\u202D', '\u202E', '\u2066', '\u2067', '\u2068', '\u2069':
			return ErrSecurityViolation
		case '\uff0e', '\uff0f', '\u3002', '\uff3c': // fullwidth dot, slash, ideographic stop, fullwidth backslash
			return ErrSecurityViolation
		}
	}

	// Reject root escapes / absolute path indicators
	if strings.HasPrefix(key, "/") || strings.HasPrefix(key, "\\") {
		return ErrSecurityViolation
	}

	// Check raw path segments
	if err := checkPathSegments(key); err != nil {
		return err
	}

	// Check URL-decoded path segments iteratively (up to 3 layers) to prevent double/multi-encoded traversal (e.g. %252e%252e)
	current := key
	for depth := 0; depth < 3 && strings.Contains(current, "%"); depth++ {
		unescaped, err := url.PathUnescape(current)
		if err != nil {
			return ErrSecurityViolation
		}
		if unescaped == current {
			break
		}
		// Validate unescaped form for control chars, root escapes, and traversal
		for i := 0; i < len(unescaped); i++ {
			b := unescaped[i]
			if b < 0x20 || b == 0x7F {
				return ErrSecurityViolation
			}
		}
		if strings.HasPrefix(unescaped, "/") || strings.HasPrefix(unescaped, "\\") {
			return ErrSecurityViolation
		}
		if err := checkPathSegments(unescaped); err != nil {
			return err
		}
		current = unescaped
	}

	return nil
}

func checkPathSegments(k string) error {
	// Normalize backslashes to slashes to catch Windows-style path traversals
	normalized := strings.ReplaceAll(k, "\\", "/")
	segs := strings.Split(normalized, "/")
	for _, seg := range segs {
		cleanSeg := strings.TrimSpace(seg)
		if cleanSeg == ".." || cleanSeg == "." {
			return ErrSecurityViolation
		}
	}
	return nil
}
