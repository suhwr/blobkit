package webdav_test

import (
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/testutil"
)

func TestWebDAVDriver_Contract(t *testing.T) {
	srv := newMockWebDAVServer()
	driver, server := setupTestDriver(t, srv)
	defer server.Close()
	defer driver.Close()

	testutil.RunDriverContractTests(t, func(t *testing.T) (blobkit.Driver, func()) {
		return driver, func() {}
	}, testutil.DriverContractOptions{
		SkipMultipart:   true,
		SkipConditional: true,
	})
}
