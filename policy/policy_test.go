package policy_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/policy"
)

func TestPolicy_MIMEAndExtensionRules(t *testing.T) {
	ctx := context.Background()

	pol := &policy.Policy{
		AllowedMIMEs:      []string{"image/*", "application/pdf"},
		DeniedExtensions:  []string{".exe", ".sh", ".php"},
		AllowedExtensions: []string{".jpg", ".png", ".pdf"},
		MaxObjectSize:     1024 * 1024, // 1MB
	}

	// 1. Valid image
	_, err := pol.Validate(ctx, policy.ValidationInput{
		Filename:    "avatar.png",
		ContentType: "image/png",
		Size:        500 * 1024,
	})
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	// 2. Denied extension
	_, err = pol.Validate(ctx, policy.ValidationInput{
		Filename:    "script.sh",
		ContentType: "text/x-sh",
		Size:        100,
	})
	if !errors.Is(err, blobkit.ErrSecurityViolation) {
		t.Fatalf("expected ErrSecurityViolation for .sh, got %v", err)
	}

	// 3. Size exceeding limit
	_, err = pol.Validate(ctx, policy.ValidationInput{
		Filename:    "large.pdf",
		ContentType: "application/pdf",
		Size:        2 * 1024 * 1024,
	})
	if !errors.Is(err, blobkit.ErrUploadTooLarge) {
		t.Fatalf("expected ErrUploadTooLarge, got %v", err)
	}
}

func TestPolicy_SanitizeAndInjectionDefense(t *testing.T) {
	// 1. Path traversal in filename
	unsafeFilename := "../../etc/passwd"
	sanitized := policy.SanitizeFilename(unsafeFilename)
	if sanitized != "passwd" {
		t.Fatalf("expected 'passwd', got %q", sanitized)
	}

	// 2. CRLF header injection and null byte stripping
	maliciousHeader := "attachment; filename=\"safe.jpg\"\x00\r\nSet-Cookie: stolen=123"
	cleaned := policy.SanitizeHeader(maliciousHeader)
	if cleaned == maliciousHeader || len(cleaned) == 0 || strings.Contains(cleaned, "\x00") {
		t.Fatalf("CRLF / null-byte header was not properly sanitized: %q", cleaned)
	}

	// 3. Path traversal detection via Validate
	pol := &policy.Policy{}
	_, err := pol.Validate(context.Background(), policy.ValidationInput{
		Filename: "../secret.txt",
	})
	if !errors.Is(err, blobkit.ErrSecurityViolation) {
		t.Fatalf("expected ErrSecurityViolation for traversal, got %v", err)
	}
}

func TestPolicy_MIMEMismatch(t *testing.T) {
	pol := &policy.Policy{
		DisallowMIMEMismatch: true,
	}

	// Deceptive payload: .jpg extension but text/html content
	_, err := pol.Validate(context.Background(), policy.ValidationInput{
		Filename:    "trojan.jpg",
		ContentType: "text/html",
	})
	if !errors.Is(err, blobkit.ErrMIMEMismatch) {
		t.Fatalf("expected ErrMIMEMismatch, got %v", err)
	}
}

func TestPolicy_MinObjectSize_ZeroByteObject(t *testing.T) {
	pol := &policy.Policy{
		MinObjectSize: 100,
	}

	// Known zero-byte object must fail MinObjectSize
	_, err := pol.Validate(context.Background(), policy.ValidationInput{
		Size:         0,
		ExplicitSize: true,
	})
	if !errors.Is(err, blobkit.ErrSecurityViolation) {
		t.Fatalf("expected ErrSecurityViolation for 0-byte object under MinObjectSize=100, got: %v", err)
	}

	// Unknown size (SizeUnknown = -1) should not fail pre-flight validation
	_, err = pol.Validate(context.Background(), policy.ValidationInput{
		Size: blobkit.SizeUnknown,
	})
	if err != nil {
		t.Fatalf("unexpected error for unknown size: %v", err)
	}
}
