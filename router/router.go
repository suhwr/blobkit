package router

import (
	"github.com/suhwr/blobkit"
)

// Re-export canonical router definitions from the root blobkit package.
type Operation = blobkit.Operation

const (
	OpPut     = blobkit.OpPut
	OpGet     = blobkit.OpGet
	OpHead    = blobkit.OpHead
	OpDelete  = blobkit.OpDelete
	OpList    = blobkit.OpList
	OpPresign = blobkit.OpPresign
)

type RouteContext = blobkit.RouteContext
type Router = blobkit.Router
