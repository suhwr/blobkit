package blobkit

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	// DefaultMaxFilenameLength is the maximum allowed client filename length.
	DefaultMaxFilenameLength = 255

	// Dangerous control characters and shell metacharacters
	reControlChars = regexp.MustCompile(`[\x00-\x1f\x7f]`)
	reCRLF         = regexp.MustCompile(`[\r\n]`)
)

// ValidationInput supplies parameters for policy enforcement.
type ValidationInput struct {
	Namespace    string
	Filename     string
	ContentType  string
	Size         int64
	ExplicitSize bool // ExplicitSize indicates that Size was explicitly specified (even if 0)
	SizeKnown    bool // SizeKnown indicates that the payload size is deterministically known
	StreamReader io.Reader
}

// Policy enforces upload security constraints, format whitelists, and size boundaries.
type Policy struct {
	// AllowedMIMEs is a whitelist of permitted MIME types (e.g. ["image/jpeg", "image/*", "audio/*"]).
	AllowedMIMEs []string

	// DeniedMIMEs is a blacklist of forbidden MIME types (e.g. ["application/x-dosexec", "application/x-sh"]).
	DeniedMIMEs []string

	// AllowedExtensions is a whitelist of permitted file extensions (e.g. [".jpg", ".png", ".pdf"]).
	AllowedExtensions []string

	// DeniedExtensions is a blacklist of forbidden extensions (e.g. [".exe", ".sh", ".bat", ".cmd", ".php"]).
	DeniedExtensions []string

	// MaxObjectSize is the maximum allowed payload size in bytes (0 means unlimited).
	MaxObjectSize int64

	// MinObjectSize is the minimum required payload size in bytes (0 means no minimum).
	MinObjectSize int64

	// MaxFilenameLength is the maximum allowed filename length (defaults to 255).
	MaxFilenameLength int

	// DisallowMIMEMismatch flags whether an obvious mismatch between file extension and detected MIME is forbidden.
	DisallowMIMEMismatch bool

	// ContentScanner is an optional custom scanner hook (e.g. antivirus or deep payload inspection).
	// It MUST return an io.Reader that allows subsequent operations to read the full payload,
	// either by returning the original reader unconsumed, or by returning a new reader that buffers/replays the consumed bytes.
	ContentScanner func(ctx context.Context, r io.Reader, filename, mime string) (io.Reader, error)
}

// Validate executes policy rules against the given input.
// It returns a potentially replacement io.Reader if the ContentScanner consumes bytes.
func (p *Policy) Validate(ctx context.Context, in ValidationInput) (io.Reader, error) {
	if p == nil {
		return in.StreamReader, nil
	}

	maxLen := p.MaxFilenameLength
	if maxLen <= 0 {
		maxLen = DefaultMaxFilenameLength
	}

	// 1. Filename validation
	if in.Filename != "" {
		if len(in.Filename) > maxLen {
			return nil, fmt.Errorf("%w: filename exceeds maximum length of %d", ErrInvalidFilename, maxLen)
		}
		if reControlChars.MatchString(in.Filename) || strings.Contains(in.Filename, "\x00") {
			return nil, fmt.Errorf("%w: filename contains control characters or null bytes", ErrSecurityViolation)
		}
		if strings.Contains(in.Filename, "..") {
			return nil, fmt.Errorf("%w: path traversal detected in filename", ErrSecurityViolation)
		}
	}

	// 2. Size boundary check (if size is known)
	sizeKnown := in.ExplicitSize || in.SizeKnown || in.Size > 0
	if in.Size == SizeUnknown {
		sizeKnown = false
	}
	if sizeKnown {
		if p.MaxObjectSize > 0 && in.Size > p.MaxObjectSize {
			return nil, fmt.Errorf("%w: size %d exceeds policy limit %d", ErrUploadTooLarge, in.Size, p.MaxObjectSize)
		}
		if p.MinObjectSize > 0 && in.Size < p.MinObjectSize {
			return nil, fmt.Errorf("%w: size %d is below minimum required %d", ErrSecurityViolation, in.Size, p.MinObjectSize)
		}
	}

	// 3. Extension policy
	if in.Filename != "" {
		ext := strings.ToLower(filepath.Ext(in.Filename))
		if len(p.DeniedExtensions) > 0 {
			for _, denied := range p.DeniedExtensions {
				if ext == strings.ToLower(denied) {
					return nil, fmt.Errorf("%w: file extension %q is forbidden by security policy", ErrSecurityViolation, ext)
				}
			}
		}

		if len(p.AllowedExtensions) > 0 {
			allowed := false
			for _, a := range p.AllowedExtensions {
				if ext == strings.ToLower(a) {
					allowed = true
					break
				}
			}
			if !allowed {
				return nil, fmt.Errorf("%w: file extension %q is not in allowed extensions list", ErrSecurityViolation, ext)
			}
		}
	}

	// 4. MIME type policy
	if in.ContentType != "" {
		cleanMIME := strings.ToLower(strings.TrimSpace(in.ContentType))

		if len(p.DeniedMIMEs) > 0 {
			for _, denied := range p.DeniedMIMEs {
				if matchMIME(cleanMIME, denied) {
					return nil, fmt.Errorf("%w: MIME type %q is denied by policy", ErrSecurityViolation, cleanMIME)
				}
			}
		}

		if len(p.AllowedMIMEs) > 0 {
			allowed := false
			for _, a := range p.AllowedMIMEs {
				if matchMIME(cleanMIME, a) {
					allowed = true
					break
				}
			}
			if !allowed {
				return nil, fmt.Errorf("%w: MIME type %q is not allowed by policy", ErrSecurityViolation, cleanMIME)
			}
		}

		// 5. Obvious MIME / extension mismatch detection
		if p.DisallowMIMEMismatch && in.Filename != "" {
			if err := checkMIMEMismatch(in.Filename, cleanMIME); err != nil {
				return nil, err
			}
		}
	}

	// 6. Optional ContentScanner hook
	if p.ContentScanner != nil && in.StreamReader != nil {
		newReader, err := p.ContentScanner(ctx, in.StreamReader, in.Filename, in.ContentType)
		if err != nil {
			return nil, fmt.Errorf("%w: content scan failed: %v", ErrSecurityViolation, err)
		}
		return newReader, nil
	}

	return in.StreamReader, nil
}

// SanitizeFilename removes directory components, null bytes, CRLF characters, and path traversals.
func SanitizeFilename(raw string) string {
	if raw == "" {
		return ""
	}
	raw = strings.ReplaceAll(raw, "\\", "/")
	cleaned := filepath.Base(raw)
	cleaned = strings.ReplaceAll(cleaned, "\x00", "")
	cleaned = reCRLF.ReplaceAllString(cleaned, "")
	cleaned = reControlChars.ReplaceAllString(cleaned, "")
	cleaned = strings.TrimSpace(cleaned)
	if cleaned == "." || cleaned == ".." || cleaned == "/" {
		return ""
	}
	return cleaned
}

// SanitizeHeader removes CRLF characters and null bytes to prevent HTTP response splitting / header injection.
func SanitizeHeader(raw string) string {
	if raw == "" {
		return ""
	}
	raw = strings.ReplaceAll(raw, "\x00", "")
	return reCRLF.ReplaceAllString(raw, " ")
}

func matchMIME(actual, pattern string) bool {
	pattern = strings.ToLower(pattern)
	if pattern == "*/*" || pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "/*") {
		prefix := strings.TrimSuffix(pattern, "/*")
		return strings.HasPrefix(actual, prefix+"/")
	}
	return actual == pattern
}

func checkMIMEMismatch(filename, mime string) error {
	ext := strings.ToLower(filepath.Ext(filename))
	if ext == "" {
		return nil
	}

	// Flag obvious deceptive mismatches: e.g. claiming image extension with HTML or executable payload
	if (ext == ".jpg" || ext == ".jpeg" || ext == ".png" || ext == ".webp") &&
		(strings.HasPrefix(mime, "text/html") || strings.Contains(mime, "x-dosexec") || strings.Contains(mime, "x-sh")) {
		return fmt.Errorf("%w: image extension %q conflicts with payload type %q", ErrMIMEMismatch, ext, mime)
	}

	if (ext == ".mp3" || ext == ".ogg" || ext == ".wav") &&
		(strings.HasPrefix(mime, "text/") || strings.Contains(mime, "x-dosexec")) {
		return fmt.Errorf("%w: audio extension %q conflicts with payload type %q", ErrMIMEMismatch, ext, mime)
	}

	return nil
}
