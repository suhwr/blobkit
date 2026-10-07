package fs

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/suhwr/blobkit"
)

// resolvePath sanitizes the object key and guarantees that the resulting absolute path
// is strictly contained within rootDir, rejecting any path traversal attempts.
func resolvePath(rootDir, key, stagingDir string) (string, error) {
	if err := blobkit.ValidateKey(key); err != nil {
		return "", err
	}

	key = strings.TrimSpace(key)

	// Normalize path separators to local OS convention
	cleanKey := filepath.Clean(filepath.FromSlash(key))

	// Prevent directory traversal or absolute path escapes
	if cleanKey == "." || cleanKey == "/" || cleanKey == "\\" || filepath.IsAbs(cleanKey) || strings.HasPrefix(cleanKey, "..") {
		return "", blobkit.WrapError("path", key, "fs", blobkit.ErrSecurityViolation)
	}

	// Prevent direct user access to internal staging or metadata files
	firstSeg := strings.Split(cleanKey, string(filepath.Separator))[0]
	if firstSeg == stagingDir || strings.HasSuffix(cleanKey, ".meta.json") {
		return "", blobkit.WrapError("path", key, "fs", blobkit.ErrSecurityViolation)
	}

	fullPath := filepath.Join(rootDir, cleanKey)

	// Strict chroot containment check: fullPath MUST have rootDir + separator as prefix
	expectedPrefix := rootDir + string(filepath.Separator)
	if !strings.HasPrefix(fullPath, expectedPrefix) {
		return "", blobkit.WrapError("path", key, "fs", blobkit.ErrSecurityViolation)
	}

	// Symlink escape check: evaluate symlinks on existing ancestors
	if realRoot, err := filepath.EvalSymlinks(rootDir); err == nil {
		checkPath := fullPath
		for {
			if target, err := filepath.EvalSymlinks(checkPath); err == nil {
				rel, err := filepath.Rel(realRoot, target)
				if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
					return "", blobkit.WrapError("path", key, "fs", blobkit.ErrSecurityViolation)
				}
				break
			}
			parent := filepath.Dir(checkPath)
			if parent == checkPath || parent == rootDir {
				break
			}
			checkPath = parent
		}
	}

	return fullPath, nil
}

// metaPath returns the associated sidecar metadata file path for a given object path.
func metaPath(fullPath string) string {
	return fullPath + ".meta.json"
}

// ensureParentDir ensures the directory containing targetPath exists.
func ensureParentDir(targetPath string, mode os.FileMode) error {
	dir := filepath.Dir(targetPath)
	return os.MkdirAll(dir, mode)
}
