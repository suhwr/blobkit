package memory_test

import (
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/memory"
	"github.com/suhwr/blobkit/testutil"
)

func TestMemoryDriver_Contract(t *testing.T) {
	testutil.RunDriverContractTests(t, func(t *testing.T) (blobkit.Driver, func()) {
		d := memory.NewDriver(memory.Config{
			Name:   "contract-memory",
			Bucket: "contract-bucket",
		})
		return d, func() {
			_ = d.Close()
		}
	})
}
