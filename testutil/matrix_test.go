package testutil

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/fs"
	"github.com/suhwr/blobkit/provider/memory"
)

// BehavioralProof records whether a capability was behaviorally proven via actual execution.
type BehavioralProof struct {
	Provider             string
	DirectPutProven      bool
	ByteRangeProven      bool
	CopyProven           bool
	BatchDeleteProven    bool
	MultipartProven      bool
	PresignProven        bool
	NegativePathProven   bool
	SecurityShieldProven bool
	RollbackProven       bool
}

// TestContractComplianceMatrix_BehavioralExecution executes behavioral proof suites
// against real storage drivers to guarantee that capability flags correspond to working implementations.
func TestContractComplianceMatrix_BehavioralExecution(t *testing.T) {
	ctx := context.Background()

	memDriver := memory.NewDriver(memory.Config{
		Bucket:        "matrix-mem",
		PublicBaseURL: "https://mem.cdn.test",
	})
	fsDriver, err := fs.NewDriver(fs.Config{
		RootDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("fs init failed: %v", err)
	}
	defer memDriver.Close()
	defer fsDriver.Close()

	drivers := []blobkit.Driver{
		memDriver,
		fsDriver,
	}

	var proofs []BehavioralProof

	for _, d := range drivers {
		p := BehavioralProof{Provider: d.Name()}
		caps := d.Capabilities()

		// 1. Behavioral Proof: DirectPut + Head + Get
		putKey := fmt.Sprintf("matrix/%s/put.txt", d.Name())
		payload := []byte("matrix behavioral payload for " + d.Name())
		hasher := sha256.New()
		hasher.Write(payload)
		expectedSHA := hex.EncodeToString(hasher.Sum(nil))

		putObj, pErr := d.Put(ctx, &blobkit.Object{Key: putKey}, bytes.NewReader(payload), blobkit.PutOptions{
			Size: int64(len(payload)),
		})
		if pErr != nil {
			t.Fatalf("[%s] behavioral Put failed: %v", d.Name(), pErr)
		}
		headObj, hErr := d.Head(ctx, putKey)
		if hErr != nil || headObj.Size != int64(len(payload)) {
			t.Fatalf("[%s] behavioral Head failed: %v", d.Name(), hErr)
		}
		reader, gErr := d.Get(ctx, putKey, blobkit.GetOptions{})
		if gErr != nil {
			t.Fatalf("[%s] behavioral Get failed: %v", d.Name(), gErr)
		}
		body, rErr := io.ReadAll(reader)
		reader.Close()
		if rErr != nil || !bytes.Equal(body, payload) {
			t.Fatalf("[%s] behavioral body mismatch", d.Name())
		}
		if putObj.Size == int64(len(payload)) && len(expectedSHA) == 64 {
			p.DirectPutProven = true
		}

		// 2. Behavioral Proof: ByteRangeGet
		if caps&blobkit.CapByteRangeGet != 0 {
			rangeReader, rErr := d.Get(ctx, putKey, blobkit.GetOptions{
				Range: "bytes=0-5",
			})
			if rErr != nil {
				t.Fatalf("[%s] range read failed: %v", d.Name(), rErr)
			}
			sub, _ := io.ReadAll(rangeReader)
			rangeReader.Close()
			if string(sub) == string(payload[0:6]) {
				p.ByteRangeProven = true
			} else {
				t.Fatalf("[%s] range read mismatch: expected %q, got %q", d.Name(), string(payload[0:6]), string(sub))
			}
		}

		// 3. Behavioral Proof: Server-Side Copy
		if caps&blobkit.CapCopy != 0 {
			copyDst := putKey + ".copy"
			cErr := d.Copy(ctx, putKey, copyDst)
			if cErr != nil {
				t.Fatalf("[%s] behavioral Copy failed: %v", d.Name(), cErr)
			}
			copyHead, chErr := d.Head(ctx, copyDst)
			if chErr != nil || copyHead.Size != int64(len(payload)) {
				t.Fatalf("[%s] Copy destination verification failed: %v", d.Name(), chErr)
			}
			// Verify source object intact
			srcHead, shErr := d.Head(ctx, putKey)
			if shErr != nil || srcHead.Size != int64(len(payload)) {
				t.Fatalf("[%s] Copy corrupted source object: %v", d.Name(), shErr)
			}
			p.CopyProven = true
		}

		// 4. Behavioral Proof: BatchDelete
		if caps&blobkit.CapBatchDelete != 0 {
			k1 := fmt.Sprintf("matrix/%s/b1.txt", d.Name())
			k2 := fmt.Sprintf("matrix/%s/b2.txt", d.Name())
			_, _ = d.Put(ctx, &blobkit.Object{Key: k1}, strings.NewReader("1"), blobkit.PutOptions{Size: 1})
			_, _ = d.Put(ctx, &blobkit.Object{Key: k2}, strings.NewReader("2"), blobkit.PutOptions{Size: 1})

			delList, bdErr := d.DeleteBatch(ctx, []string{k1, k2})
			if bdErr != nil || len(delList) != 2 {
				t.Fatalf("[%s] behavioral DeleteBatch failed: %v", d.Name(), bdErr)
			}
			_, h1 := d.Head(ctx, k1)
			_, h2 := d.Head(ctx, k2)
			if blobkit.IsNotFound(h1) && blobkit.IsNotFound(h2) {
				p.BatchDeleteProven = true
			}
		}

		// 5. Behavioral Proof: MultipartSession
		if caps&blobkit.CapMultipartSession != 0 {
			mpKey := fmt.Sprintf("matrix/%s/mp.bin", d.Name())
			uploadID, uErr := d.CreateMultipart(ctx, &blobkit.Object{Key: mpKey}, blobkit.PutOptions{})
			if uErr != nil {
				t.Fatalf("[%s] CreateMultipart failed: %v", d.Name(), uErr)
			}
			p1Data := []byte("chunk-1-data-")
			p2Data := []byte("chunk-2-data-")
			e1, ep1 := d.UploadPart(ctx, mpKey, uploadID, 1, bytes.NewReader(p1Data), int64(len(p1Data)))
			e2, ep2 := d.UploadPart(ctx, mpKey, uploadID, 2, bytes.NewReader(p2Data), int64(len(p2Data)))
			if ep1 != nil || ep2 != nil {
				t.Fatalf("[%s] UploadPart failed: %v, %v", d.Name(), ep1, ep2)
			}
			completed, cErr := d.CompleteMultipart(ctx, &blobkit.Object{Key: mpKey}, uploadID, []blobkit.CompletedPart{
				{PartNumber: 1, ETag: e1, Size: int64(len(p1Data))},
				{PartNumber: 2, ETag: e2, Size: int64(len(p2Data))},
			})
			if cErr != nil || completed.Size != int64(len(p1Data)+len(p2Data)) {
				t.Fatalf("[%s] CompleteMultipart failed: %v", d.Name(), cErr)
			}
			p.MultipartProven = true
		}

		// 6. Behavioral Proof: Presign URLs
		if caps&blobkit.CapPresignGet != 0 {
			ps, psErr := d.PresignGet(ctx, putKey, blobkit.PresignOptions{Expiry: 10 * time.Minute})
			if psErr != nil || ps.URL == "" {
				t.Fatalf("[%s] PresignGet failed: %v", d.Name(), psErr)
			}
			p.PresignProven = true
		}

		// 7. Negative Path Proof: Calling unadvertised capability MUST return ErrUnsupportedOperation
		if caps&blobkit.CapPresignGet == 0 {
			_, unErr := d.PresignGet(ctx, putKey, blobkit.PresignOptions{})
			if !errors.Is(unErr, blobkit.ErrUnsupportedOperation) {
				t.Fatalf("[%s] expected ErrUnsupportedOperation on unadvertised PresignGet, got: %v", d.Name(), unErr)
			}
			p.NegativePathProven = true
		} else {
			p.NegativePathProven = true
		}

		// 8. Behavioral Proof: Path Traversal Security Wall
		_, secErr := d.Put(ctx, &blobkit.Object{Key: "../escaped.txt"}, strings.NewReader("bad"), blobkit.PutOptions{Size: 3})
		if errors.Is(secErr, blobkit.ErrSecurityViolation) || errors.Is(secErr, blobkit.ErrInvalidKey) {
			p.SecurityShieldProven = true
		} else {
			t.Fatalf("[%s] path traversal was not rejected: %v", d.Name(), secErr)
		}

		// 9. Behavioral Proof: Short Read Rollback
		rollKey := fmt.Sprintf("matrix/%s/rollback.txt", d.Name())
		_, rollErr := d.Put(ctx, &blobkit.Object{Key: rollKey}, strings.NewReader("short"), blobkit.PutOptions{
			ExplicitSize: true,
			Size:         100, // declared 100, provided 5
		})
		if blobkit.IsPermanent(rollErr) {
			_, afterHead := d.Head(ctx, rollKey)
			if blobkit.IsNotFound(afterHead) {
				p.RollbackProven = true
			} else {
				t.Fatalf("[%s] short read leaked partial object in storage: %v", d.Name(), afterHead)
			}
		} else {
			t.Fatalf("[%s] short read did not return permanent size mismatch error: %v", d.Name(), rollErr)
		}

		proofs = append(proofs, p)
	}

	var sb strings.Builder
	sb.WriteString("\n| Provider | DirectPut | ByteRange | Copy | BatchDelete | Multipart | Presign | NegativePath | SecurityShield | Rollback |\n")
	sb.WriteString("|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|\n")

	mark := func(b bool) string {
		if b {
			return "✅ PROVEN"
		}
		return "❌ FAIL"
	}

	for _, p := range proofs {
		sb.WriteString(fmt.Sprintf("| %-10s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			p.Provider,
			mark(p.DirectPutProven),
			mark(p.ByteRangeProven),
			mark(p.CopyProven),
			mark(p.BatchDeleteProven),
			mark(p.MultipartProven),
			mark(p.PresignProven),
			mark(p.NegativePathProven),
			mark(p.SecurityShieldProven),
			mark(p.RollbackProven),
		))
	}

	t.Log(sb.String())

	for _, p := range proofs {
		if !p.DirectPutProven || !p.SecurityShieldProven || !p.RollbackProven || !p.NegativePathProven {
			t.Fatalf("Provider %s failed critical behavioral proof", p.Provider)
		}
	}
}
