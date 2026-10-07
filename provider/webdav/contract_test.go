package webdav_test

import (
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/testutil"
)

func TestWebDAVDriver_Contract(t *testing.T) {
	testutil.RunDriverContractTests(t, func(t *testing.T) (blobkit.Driver, func()) {
		srv := newMockWebDAVServer()
		driver, server := setupTestDriver(t, srv)
		return driver, func() {
			_ = driver.Close()
			server.Close()
		}
	}, testutil.DriverContractOptions{
		SkipMultipart:   true,
		SkipConditional: true,
	})
}
