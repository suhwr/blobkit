package sftp

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/pkg/sftp"
	"github.com/suhwr/blobkit"
)

var (
	privateKeyPemRegex = regexp.MustCompile(`(?s)-----BEGIN[^\-]+PRIVATE KEY-----.*?-----END[^\-]+PRIVATE KEY-----`)
	passwordScrubRegex = regexp.MustCompile(`(?i)(password[:=]\s*)[^\s,;&]+`)
)

// scrubCredentials sanitizes sensitive data such as private keys and passwords from error messages.
func scrubCredentials(msg string) string {
	if msg == "" {
		return ""
	}
	s := privateKeyPemRegex.ReplaceAllString(msg, "[REDACTED PRIVATE KEY]")
	s = passwordScrubRegex.ReplaceAllString(s, "$1[REDACTED]")
	return s
}

// mapSFTPError maps low-level SFTP, SSH, and OS filesystem errors to standard BlobKit domain errors.
func mapSFTPError(op, key, driverName string, err error) error {
	if err == nil {
		return nil
	}

	if blobkit.PreserveSentinel(err) {
		return blobkit.WrapError(op, key, driverName, err)
	}

	scrubbedMsg := scrubCredentials(err.Error())

	// Object Not Found checks
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, sftp.ErrSSHFxNoSuchFile) {
		return blobkit.WrapError(op, key, driverName, blobkit.ErrObjectNotFound)
	}

	// Permission checks
	if errors.Is(err, os.ErrPermission) || errors.Is(err, sftp.ErrSSHFxPermissionDenied) {
		return blobkit.WrapError(op, key, driverName, fmt.Errorf("%w: %s", blobkit.ErrPermissionDenied, scrubbedMsg))
	}

	// Connection drop / network failures
	if errors.Is(err, io.EOF) || strings.Contains(scrubbedMsg, "broken pipe") || strings.Contains(scrubbedMsg, "connection reset") {
		return blobkit.WrapError(op, key, driverName, fmt.Errorf("%w: sftp connection dropped: %s", blobkit.ErrProviderUnavailable, scrubbedMsg))
	}

	// Default fallback
	return blobkit.WrapError(op, key, driverName, errors.New(scrubbedMsg))
}
