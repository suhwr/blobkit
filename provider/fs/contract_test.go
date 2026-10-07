package fs_test

import (
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/fs"
	"github.com/suhwr/blobkit/testutil"
)

func TestFSDriver_Contract(t *testing.T) {
	testutil.RunDriverContractTests(t, func(t *testing.T) (blobkit.Driver, func()) {
		tmpDir := t.TempDir()
		driver, err := fs.NewDriver(fs.Config{
			Name:              "fs-contract",
			RootDir:           tmpDir,
			EnableSidecarMeta: true,
		})
		if err != nil {
			t.Fatalf("failed to create fs driver: %v", err)
		}
		return driver, func() {
			_ = driver.Close()
		}
	})
}
