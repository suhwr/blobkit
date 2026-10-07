package policy_test

import (
	"context"
	"errors"
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

	// 2. CRLF header injection
	maliciousHeader := "attachment; filename=\"safe.jpg\"\r\nSet-Cookie: stolen=123"
	cleaned := policy.SanitizeHeader(maliciousHeader)
	if cleaned == maliciousHeader || len(cleaned) == 0 {
		t.Fatal("CRLF header was not sanitized")
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
