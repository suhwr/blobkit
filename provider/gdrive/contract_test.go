package gdrive_test

import (
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/testutil"
)

func TestGDriveDriver_Contract(t *testing.T) {
	testutil.RunDriverContractTests(t, func(t *testing.T) (blobkit.Driver, func()) {
		srv := newMockDriveServer()
		driver, server := setupTestDriver(t, srv)
		return driver, func() {
			_ = driver.Close()
			server.Close()
		}
	}, testutil.DriverContractOptions{
		SkipConditional: true,
	})
}
