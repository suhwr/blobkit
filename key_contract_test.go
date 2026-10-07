package blobkit_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
)

func TestValidateKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr error
	}{
		{"empty", "", blobkit.ErrInvalidKey},
		{"whitespace", "   \t\n  ", blobkit.ErrInvalidKey},
		{"too long", strings.Repeat("a", 1025), blobkit.ErrInvalidKey},
		{"null byte", "file\x00name.txt", blobkit.ErrSecurityViolation},
		{"carriage return", "file\rname.txt", blobkit.ErrSecurityViolation},
		{"newline", "file\nname.txt", blobkit.ErrSecurityViolation},
		{"leading slash", "/etc/passwd", blobkit.ErrSecurityViolation},
		{"leading backslash", "\\windows\\system32", blobkit.ErrSecurityViolation},
		{"parent traversal", "../secret.txt", blobkit.ErrSecurityViolation},
		{"double parent traversal", "../../secret.txt", blobkit.ErrSecurityViolation},
		{"embedded parent traversal", "photos/../../etc/passwd", blobkit.ErrSecurityViolation},
		{"current dir", "./test.txt", blobkit.ErrSecurityViolation},
		{"embedded current dir", "photos/./test.txt", blobkit.ErrSecurityViolation},
		{"windows backslash traversal", "photos\\..\\secret.txt", blobkit.ErrSecurityViolation},
		{"encoded traversal %2e%2e", "%2e%2e/secret.txt", blobkit.ErrSecurityViolation},
		{"encoded traversal uppercase %2E%2E", "%2E%2E/secret.txt", blobkit.ErrSecurityViolation},
		{"valid key simple", "test.txt", nil},
		{"valid key with path", "avatars/2026/01/user_123.png", nil},
		{"valid key with special chars", "reports/annual report (final) [v2].pdf", nil},
		{"valid key unicode russian", "документы/отчет.pdf", nil},
		{"valid key unicode japanese", "写真/旅行/富士山.jpg", nil},
		{"valid key repeated slashes", "folder//file.txt", nil},
		{"valid key trailing slash", "directory/", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := blobkit.ValidateKey(tt.key)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("expected nil error for key %q, got: %v", tt.key, err)
				}
			} else {
				if err == nil {
					t.Fatalf("expected error %v for key %q, got nil", tt.wantErr, tt.key)
				}
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected error %v for key %q, got %v", tt.wantErr, tt.key, err)
				}
			}
		})
	}
}

func TestSizeReader_ExactMatch(t *testing.T) {
	data := []byte("hello world")
	sr := blobkit.NewSizeReader(bytes.NewReader(data), int64(len(data)), true)

	buf, err := io.ReadAll(sr)
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Fatalf("data mismatch: expected %q, got %q", data, buf)
	}
	if err := sr.Verify(); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
}

func TestSizeReader_ShortStream(t *testing.T) {
	data := []byte("short")
	sr := blobkit.NewSizeReader(bytes.NewReader(data), 10, true)

	_, err := io.ReadAll(sr)
	if err == nil {
		t.Fatal("expected ErrSizeMismatch, got nil")
	}
	if !errors.Is(err, blobkit.ErrSizeMismatch) {
		t.Fatalf("expected ErrSizeMismatch, got: %v", err)
	}
}

func TestSizeReader_LongStream(t *testing.T) {
	data := []byte("extra long stream")
	sr := blobkit.NewSizeReader(bytes.NewReader(data), 5, true)

	_, err := io.ReadAll(sr)
	if err == nil {
		t.Fatal("expected ErrSizeMismatch, got nil")
	}
	if !errors.Is(err, blobkit.ErrSizeMismatch) {
		t.Fatalf("expected ErrSizeMismatch, got: %v", err)
	}
}

func TestResolvePayload_ZeroByte(t *testing.T) {
	reader, size, exact, err := blobkit.ResolvePayload(strings.NewReader(""), blobkit.PutOptions{})
	if err != nil {
		t.Fatalf("ResolvePayload failed: %v", err)
	}
	if size != 0 || !exact {
		t.Fatalf("expected size 0 and exact=true, got size=%d, exact=%v", size, exact)
	}
	buf, err := io.ReadAll(reader)
	if err != nil || len(buf) != 0 {
		t.Fatalf("expected empty read, got len=%d, err=%v", len(buf), err)
	}
}

func TestResolvePayload_ExplicitZeroByte(t *testing.T) {
	reader, size, exact, err := blobkit.ResolvePayload(bytes.NewReader(nil), blobkit.PutOptions{
		Size:         0,
		ExplicitSize: true,
	})
	if err != nil {
		t.Fatalf("ResolvePayload failed: %v", err)
	}
	if size != 0 || !exact {
		t.Fatalf("expected size 0 and exact=true, got size=%d, exact=%v", size, exact)
	}
	buf, err := io.ReadAll(reader)
	if err != nil || len(buf) != 0 {
		t.Fatalf("expected empty read, got len=%d, err=%v", len(buf), err)
	}
}

func TestCheckPreconditions(t *testing.T) {
	etag := "\"12345678\""
	now := time.Now().UTC()
	past := now.Add(-10 * time.Minute)
	future := now.Add(10 * time.Minute)

	// IfMatch match
	if err := blobkit.CheckPreconditions(etag, now, blobkit.GetOptions{IfMatch: "\"12345678\""}); err != nil {
		t.Fatalf("expected match, got err: %v", err)
	}
	// IfMatch mismatch
	if err := blobkit.CheckPreconditions(etag, now, blobkit.GetOptions{IfMatch: "\"wrong\""}); !errors.Is(err, blobkit.ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed, got: %v", err)
	}

	// IfNoneMatch match -> fail
	if err := blobkit.CheckPreconditions(etag, now, blobkit.GetOptions{IfNoneMatch: "\"12345678\""}); !errors.Is(err, blobkit.ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed, got: %v", err)
	}
	// IfNoneMatch mismatch -> success
	if err := blobkit.CheckPreconditions(etag, now, blobkit.GetOptions{IfNoneMatch: "\"wrong\""}); err != nil {
		t.Fatalf("expected nil, got: %v", err)
	}

	// IfModifiedSince
	if err := blobkit.CheckPreconditions(etag, now, blobkit.GetOptions{IfModifiedSince: &past}); err != nil {
		t.Fatalf("expected nil for modified since past, got: %v", err)
	}
	if err := blobkit.CheckPreconditions(etag, now, blobkit.GetOptions{IfModifiedSince: &future}); !errors.Is(err, blobkit.ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed for future modified since, got: %v", err)
	}

	// IfUnmodifiedSince
	if err := blobkit.CheckPreconditions(etag, now, blobkit.GetOptions{IfUnmodifiedSince: &future}); err != nil {
		t.Fatalf("expected nil for unmodified since future, got: %v", err)
	}
	if err := blobkit.CheckPreconditions(etag, now, blobkit.GetOptions{IfUnmodifiedSince: &past}); !errors.Is(err, blobkit.ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed for past unmodified since, got: %v", err)
	}
}
