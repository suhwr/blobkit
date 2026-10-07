package blobkit_test

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
)

func FuzzValidateKey(f *testing.F) {
	seeds := []string{
		"avatar.png",
		"users/123/profile.jpg",
		"documents/2026/report.pdf",
		"",
		"../secret",
		"foo/../../bar",
		"/absolute/path",
		"double//slash",
		"null\x00byte",
		"back\\slash",
		"foo%2e%2ebar",
		"foo%252e%252ebar",
		"dir%2fsecret",
		"unicode\u202Eoverride",
		"fullwidth\uFF0Fslash",
		"control\r\nchar",
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, key string) {
		err := blobkit.ValidateKey(key)
		if err == nil {
			// If validation passed, assert strict security invariants
			if key == "" {
				t.Fatalf("empty key passed validation")
			}
			if strings.HasPrefix(key, "/") || strings.HasPrefix(key, "\\") {
				t.Fatalf("key starting with slash or backslash passed validation: %q", key)
			}
			if strings.Contains(key, "\x00") {
				t.Fatalf("null byte passed validation: %q", key)
			}
			for _, r := range key {
				if r < 0x20 || r == 0x7F {
					t.Fatalf("control char passed validation: %q (char %U)", key, r)
				}
				if r >= 0x202A && r <= 0x202E {
					t.Fatalf("bidi override passed validation: %q", key)
				}
				if r >= 0x2066 && r <= 0x2069 {
					t.Fatalf("bidi isolate passed validation: %q", key)
				}
				if r == 0xFF0F || r == 0xFF3C {
					t.Fatalf("fullwidth slash passed validation: %q", key)
				}
			}

			// Invariant: No segment in key or its unescaped form can be "." or ".."
			current := key
			for depth := 0; depth < 3; depth++ {
				normalized := strings.ReplaceAll(current, "\\", "/")
				for _, seg := range strings.Split(normalized, "/") {
					clean := strings.TrimSpace(seg)
					if clean == ".." || clean == "." {
						t.Fatalf("path traversal passed validation: %q (segment %q)", key, clean)
					}
				}
				unescaped, uerr := url.PathUnescape(current)
				if uerr != nil || unescaped == current {
					break
				}
				current = unescaped
			}
		}
	})
}

func FuzzSanitizeErrorMessage(f *testing.F) {
	seeds := []string{
		"normal error message",
		"dial tcp 127.0.0.1:8080: connect: connection refused",
		"dial tcp 10.0.0.5:9000: i/o timeout",
		"dial tcp [::1]:80: error",
		"Authorization: Bearer my-secret-token-12345",
		"Basic dXNlcjpwYXNz",
		"token=super_secret_api_key_123",
		"AWS AKIAIOSFODNN7EXAMPLE secret",
		"client_secret=abcdef1234567890",
		`{"client_secret":"very_secret_pass"}`,
		"-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA0...\n-----END RSA PRIVATE KEY-----",
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		sanitized := blobkit.SanitizeErrorMessage(raw)
		// Invariant: must never panic, and PEM private keys must never remain
		if strings.Contains(sanitized, "-----BEGIN") && strings.Contains(sanitized, "PRIVATE KEY-----") {
			t.Fatalf("private key leaked in sanitized error: %s", sanitized)
		}
	})
}

func FuzzCheckPreconditions(f *testing.F) {
	seeds := []struct {
		etag        string
		ifMatch     string
		ifNoneMatch string
	}{
		{`"12345"`, `"12345"`, `""`},
		{`W/"12345"`, `"12345"`, `""`},
		{`"12345"`, `*`, `""`},
		{`"12345"`, `""`, `"12345"`},
		{`"abc"`, `"xyz"`, `""`},
	}

	for _, s := range seeds {
		f.Add(s.etag, s.ifMatch, s.ifNoneMatch)
	}

	f.Fuzz(func(t *testing.T, etag, ifMatch, ifNoneMatch string) {
		modTime := time.Now().UTC()
		opts := blobkit.GetOptions{
			IfMatch:     ifMatch,
			IfNoneMatch: ifNoneMatch,
		}
		// Invariant: CheckPreconditions must never panic on arbitrary inputs
		_ = blobkit.CheckPreconditions(etag, modTime, opts)
	})
}
