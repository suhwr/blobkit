package gcs

import (
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/testutil"
)

func TestGCSDriver_Contract(t *testing.T) {
	testutil.RunDriverContractTests(t, func(t *testing.T) (blobkit.Driver, func()) {
		mock := newMockGCSServer("test-bucket")
		driver, server := setupTestDriver(t, mock)
		return driver, func() {
			_ = driver.Close()
			server.Close()
		}
	}, testutil.DriverContractOptions{
		SkipConditional: true,
	})
}
