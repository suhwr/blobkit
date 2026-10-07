package gcs

import (
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/testutil"
)

func TestGCSDriver_Contract(t *testing.T) {
	mock := newMockGCSServer("test-bucket")
	driver, server := setupTestDriver(t, mock)
	defer server.Close()
	defer driver.Close()

	testutil.RunDriverContractTests(t, func(t *testing.T) (blobkit.Driver, func()) {
		return driver, func() {}
	}, testutil.DriverContractOptions{
		SkipConditional: true,
	})
}
