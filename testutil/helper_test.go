package testutil_test

import (
	"context"
	"testing"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/testutil"
)

func TestMockStorage(t *testing.T) {
	mock, err := testutil.NewMockStorage()
	if err != nil {
		t.Fatalf("failed to create MockStorage: %v", err)
	}
	defer mock.Close()

	ctx := context.Background()
	obj, err := mock.SeedObject(ctx, "testing", "sample.txt", "hello mock storage")
	if err != nil {
		t.Fatalf("SeedObject failed: %v", err)
	}

	testutil.AssertBlobContent(t, mock.Client, obj.ID, "hello mock storage")
}

func TestRunDriverContractTests_OnMock(t *testing.T) {
	testutil.RunDriverContractTests(t, func(t *testing.T) (blobkit.Driver, func()) {
		mock, err := testutil.NewMockStorage()
		if err != nil {
			t.Fatalf("failed to create mock storage: %v", err)
		}
		return mock.Driver, func() {
			_ = mock.Close()
		}
	})
}
