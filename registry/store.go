package registry

import (
	"github.com/suhwr/blobkit"
)

// Re-export canonical registry types from the root blobkit package.
type Record = blobkit.Record
type Filter = blobkit.Filter
type Store = blobkit.MetadataStore
type UploadSession = blobkit.UploadSession
type SessionState = blobkit.SessionState
