package router

import (
	"github.com/suhwr/blobkit"
)

// Re-export canonical router definitions from the root blobkit package.
type Operation = blobkit.Operation

const (
	OpPut             = blobkit.OpPut
	OpGet             = blobkit.OpGet
	OpHead            = blobkit.OpHead
	OpDelete          = blobkit.OpDelete
	OpList            = blobkit.OpList
	OpPresign         = blobkit.OpPresign
	OpCopy            = blobkit.OpCopy
	OpMove            = blobkit.OpMove
	OpUploadPart      = blobkit.OpUploadPart
	OpMultipart       = blobkit.OpMultipart
	OpSoftDelete      = blobkit.OpSoftDelete
	OpRestore         = blobkit.OpRestore
	OpPermanentDelete = blobkit.OpPermanentDelete
)

type RouteContext = blobkit.RouteContext
type Router = blobkit.Router
