package sftp

import (
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/testutil"
)

func TestSFTPDriver_Contract(t *testing.T) {
	testutil.RunDriverContractTests(t, func(t *testing.T) (blobkit.Driver, func()) {
		driver, srv := setupTestSFTPDriver(t)
		return driver, func() {
			_ = driver.Close()
			srv.close()
		}
	}, testutil.DriverContractOptions{
		SkipMultipart: true,
	})
}
