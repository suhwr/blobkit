package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3client "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/suhwr/blobkit"
)

type partTask struct {
	number int32
	data   []byte
	bufPtr *[]byte
}

type partResult struct {
	number int32
	etag   string
	err    error
}

// uploadMultipart performs a streaming chunked upload using S3 multipart APIs
// while enforcing bounded memory consumption per upload stream and across the client.
func (d *Driver) uploadMultipart(ctx context.Context, obj *blobkit.Object, r io.Reader, opts blobkit.PutOptions) (*blobkit.Object, error) {
	createInput := &s3client.CreateMultipartUploadInput{
		Bucket:      aws.String(d.cfg.Bucket),
		Key:         aws.String(obj.Key),
		ContentType: aws.String(obj.ContentType),
	}
	if len(opts.Metadata) > 0 {
		createInput.Metadata = opts.Metadata
	}
	if opts.ContentDisposition != "" {
		createInput.ContentDisposition = aws.String(opts.ContentDisposition)
	}
	if opts.CacheControl != "" {
		createInput.CacheControl = aws.String(opts.CacheControl)
	}

	createResp, err := d.client.CreateMultipartUpload(ctx, createInput)
	if err != nil {
		return nil, d.wrapError("create_multipart", obj.Key, err)
	}
	uploadID := *createResp.UploadId

	var abortOnce sync.Once
	abortUpload := func() {
		abortOnce.Do(func() {
			abortCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_, _ = d.client.AbortMultipartUpload(abortCtx, &s3client.AbortMultipartUploadInput{
				Bucket:   aws.String(d.cfg.Bucket),
				Key:      aws.String(obj.Key),
				UploadId: aws.String(uploadID),
			})
		})
	}

	tasks := make(chan partTask, d.cfg.MultipartConcurrency)
	results := make(chan partResult, d.cfg.MultipartConcurrency)

	var wg sync.WaitGroup
	// Spawn worker pool
	for i := 0; i < d.cfg.MultipartConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				partResp, partErr := d.client.UploadPart(ctx, &s3client.UploadPartInput{
					Bucket:     aws.String(d.cfg.Bucket),
					Key:        aws.String(obj.Key),
					UploadId:   aws.String(uploadID),
					PartNumber: aws.Int32(task.number),
					Body:       bytes.NewReader(task.data),
				})

				// Return buffer to pool and release memory budget immediately after part upload finishes
				d.putChunkBuffer(task.bufPtr)
				d.cfg.GlobalMemoryLimiter.Release(int64(len(task.data)))

				if partErr != nil {
					results <- partResult{number: task.number, err: partErr}
				} else {
					etag := ""
					if partResp.ETag != nil {
						etag = *partResp.ETag
					}
					results <- partResult{number: task.number, etag: etag}
				}
			}
		}()
	}

	// Collector goroutine
	var completedParts []types.CompletedPart
	var collectErr error
	var collectWg sync.WaitGroup
	collectWg.Add(1)
	go func() {
		defer collectWg.Done()
		for res := range results {
			if res.err != nil && collectErr == nil {
				collectErr = res.err
			} else if res.err == nil {
				completedParts = append(completedParts, types.CompletedPart{
					PartNumber: aws.Int32(res.number),
					ETag:       aws.String(res.etag),
				})
			}
		}
	}()

	var totalBytes int64
	var partNum int32 = 1

	// Stream reading loop
uploadLoop:
	for {
		if ctx.Err() != nil {
			collectErr = ctx.Err()
			break
		}
		if collectErr != nil {
			break
		}

		// Allocate chunk with memory limiter budget
		chunkSize := d.cfg.MultipartPartSize
		if err := d.cfg.GlobalMemoryLimiter.Acquire(ctx, chunkSize); err != nil {
			collectErr = err
			break
		}

		bufPtr := d.getChunkBuffer()
		buf := *bufPtr
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			// If we read less than chunkSize, adjust the limiter allocation
			if int64(n) < chunkSize {
				d.cfg.GlobalMemoryLimiter.Release(chunkSize - int64(n))
			}
			data := buf[:n]
			totalBytes += int64(n)

			select {
			case tasks <- partTask{number: partNum, data: data, bufPtr: bufPtr}:
				partNum++
			case <-ctx.Done():
				d.putChunkBuffer(bufPtr)
				d.cfg.GlobalMemoryLimiter.Release(int64(n))
				collectErr = ctx.Err()
				break uploadLoop
			}
		} else {
			// No bytes read, return buffer and release full budget
			d.putChunkBuffer(bufPtr)
			d.cfg.GlobalMemoryLimiter.Release(chunkSize)
		}

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			collectErr = readErr
			break
		}
	}

	close(tasks)
	wg.Wait()
	close(results)
	collectWg.Wait()

	if collectErr != nil {
		abortUpload()
		return nil, d.wrapError("upload_multipart", obj.Key, collectErr)
	}

	if sr, ok := r.(*blobkit.SizeReader); ok {
		if verifyErr := sr.Verify(); verifyErr != nil {
			abortUpload()
			return nil, d.wrapError("upload_multipart", obj.Key, verifyErr)
		}
	}

	if len(completedParts) == 0 {
		abortUpload()
		return nil, d.wrapError("upload_multipart", obj.Key, fmt.Errorf("no parts uploaded"))
	}

	// Sort completed parts in ascending order of PartNumber (required by S3)
	sort.Slice(completedParts, func(i, j int) bool {
		return *completedParts[i].PartNumber < *completedParts[j].PartNumber
	})

	completeResp, err := d.client.CompleteMultipartUpload(ctx, &s3client.CompleteMultipartUploadInput{
		Bucket:   aws.String(d.cfg.Bucket),
		Key:      aws.String(obj.Key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		abortUpload()
		return nil, d.wrapError("complete_multipart", obj.Key, err)
	}

	now := time.Now().UTC()
	stored := *obj
	stored.Bucket = d.cfg.Bucket
	stored.Size = totalBytes
	if completeResp.ETag != nil {
		stored.ETag = *completeResp.ETag
	}
	stored.UpdatedAt = now
	if stored.CreatedAt.IsZero() {
		stored.CreatedAt = now
	}
	stored.Provider = d.cfg.Name

	return &stored, nil
}
