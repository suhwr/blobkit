package policy

import (
	"github.com/suhwr/blobkit"
)

// Re-export Policy types from root blobkit package.
type Policy = blobkit.Policy
type ValidationInput = blobkit.ValidationInput

var (
	SanitizeFilename = blobkit.SanitizeFilename
	SanitizeHeader   = blobkit.SanitizeHeader
)
