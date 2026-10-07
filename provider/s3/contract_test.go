package s3_test

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/s3"
	"github.com/suhwr/blobkit/testutil"
)

type mockS3Object struct {
	data        []byte
	contentType string
	etag        string
	modTime     time.Time
	metadata    map[string]string
}

type mockS3Server struct {
	mu         sync.RWMutex
	bucket     string
	objects    map[string]*mockS3Object
	uploads    map[string]map[int32][]byte // uploadID -> partNumber -> data
	uploadKeys map[string]string           // uploadID -> key
}

func newMockS3Server(bucket string) *mockS3Server {
	return &mockS3Server{
		bucket:     bucket,
		objects:    make(map[string]*mockS3Object),
		uploads:    make(map[string]map[int32][]byte),
		uploadKeys: make(map[string]string),
	}
}

func (s *mockS3Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	reqBucket := parts[0]
	key := ""
	if len(parts) > 1 {
		key = parts[1]
	}

	if reqBucket != s.bucket {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message></Error>`))
		return
	}

	// 1. Batch Delete: POST /{bucket}?delete
	if r.Method == http.MethodPost && r.URL.Query().Has("delete") {
		body, _ := io.ReadAll(r.Body)
		type deleteObj struct {
			Key string `xml:"Key"`
		}
		type deleteReq struct {
			Objects []deleteObj `xml:"Object"`
		}
		var delReq deleteReq
		_ = xml.Unmarshal(body, &delReq)

		var xmlBuf bytes.Buffer
		xmlBuf.WriteString(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		for _, o := range delReq.Objects {
			delete(s.objects, o.Key)
			xmlBuf.WriteString(fmt.Sprintf(`<Deleted><Key>%s</Key></Deleted>`, o.Key))
		}
		xmlBuf.WriteString(`</DeleteResult>`)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(xmlBuf.Bytes())
		return
	}

	// 2. Initiate Multipart: POST /{bucket}/{key}?uploads
	if r.Method == http.MethodPost && r.URL.Query().Has("uploads") {
		uploadID := fmt.Sprintf("mock-upload-%d", time.Now().UnixNano())
		s.uploads[uploadID] = make(map[int32][]byte)
		s.uploadKeys[uploadID] = key

		respXML := fmt.Sprintf(`<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>%s</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, s.bucket, key, uploadID)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(respXML))
		return
	}

	// 3. Complete Multipart: POST /{bucket}/{key}?uploadId=...
	if r.Method == http.MethodPost && r.URL.Query().Has("uploadId") {
		uploadID := r.URL.Query().Get("uploadId")
		stagedParts, ok := s.uploads[uploadID]
		if !ok || s.uploadKeys[uploadID] != key {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchUpload</Code></Error>`))
			return
		}

		type completedPartXML struct {
			PartNumber int32  `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		}
		type completeMultipartUploadXML struct {
			Parts []completedPartXML `xml:"Part"`
		}
		body, _ := io.ReadAll(r.Body)
		var cReq completeMultipartUploadXML
		_ = xml.Unmarshal(body, &cReq)

		sort.Slice(cReq.Parts, func(i, j int) bool {
			return cReq.Parts[i].PartNumber < cReq.Parts[j].PartNumber
		})

		var total bytes.Buffer
		for _, p := range cReq.Parts {
			partData, pExists := stagedParts[p.PartNumber]
			if !pExists {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`<Error><Code>InvalidPart</Code></Error>`))
				return
			}
			total.Write(partData)
		}

		finalData := total.Bytes()
		h := md5.Sum(finalData)
		etag := fmt.Sprintf("\"%s-%d\"", hex.EncodeToString(h[:]), len(cReq.Parts))

		s.objects[key] = &mockS3Object{
			data:        finalData,
			contentType: "application/octet-stream",
			etag:        etag,
			modTime:     time.Now().UTC(),
			metadata:    make(map[string]string),
		}
		delete(s.uploads, uploadID)
		delete(s.uploadKeys, uploadID)

		respXML := fmt.Sprintf(`<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>%s</Bucket><Key>%s</Key><ETag>%s</ETag></CompleteMultipartUploadResult>`, s.bucket, key, etag)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(respXML))
		return
	}

	// 4. Abort Multipart: DELETE /{bucket}/{key}?uploadId=...
	if r.Method == http.MethodDelete && r.URL.Query().Has("uploadId") {
		uploadID := r.URL.Query().Get("uploadId")
		delete(s.uploads, uploadID)
		delete(s.uploadKeys, uploadID)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 5. Upload Part: PUT /{bucket}/{key}?partNumber=...&uploadId=...
	if r.Method == http.MethodPut && r.URL.Query().Has("partNumber") && r.URL.Query().Has("uploadId") {
		uploadID := r.URL.Query().Get("uploadId")
		partNum, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
		stagedParts, ok := s.uploads[uploadID]
		if !ok || s.uploadKeys[uploadID] != key {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchUpload</Code></Error>`))
			return
		}

		data, _ := io.ReadAll(r.Body)
		stagedParts[int32(partNum)] = data
		h := md5.Sum(data)
		etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(h[:]))

		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusOK)
		return
	}

	// 6. List Parts: GET /{bucket}/{key}?uploadId=...
	if r.Method == http.MethodGet && r.URL.Query().Has("uploadId") {
		uploadID := r.URL.Query().Get("uploadId")
		stagedParts, ok := s.uploads[uploadID]
		if !ok || s.uploadKeys[uploadID] != key {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchUpload</Code></Error>`))
			return
		}

		var xmlBuf bytes.Buffer
		xmlBuf.WriteString(`<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		var partNums []int
		for num := range stagedParts {
			partNums = append(partNums, int(num))
		}
		sort.Ints(partNums)
		for _, num := range partNums {
			pData := stagedParts[int32(num)]
			h := md5.Sum(pData)
			etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(h[:]))
			xmlBuf.WriteString(fmt.Sprintf(`<Part><PartNumber>%d</PartNumber><ETag>%s</ETag><Size>%d</Size></Part>`, num, etag, len(pData)))
		}
		xmlBuf.WriteString(`</ListPartsResult>`)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(xmlBuf.Bytes())
		return
	}

	// 7. Copy Object: PUT with x-amz-copy-source header
	if r.Method == http.MethodPut && r.Header.Get("x-amz-copy-source") != "" {
		copySrc := r.Header.Get("x-amz-copy-source")
		copySrc = strings.TrimPrefix(copySrc, "/")
		srcParts := strings.SplitN(copySrc, "/", 2)
		srcKey := ""
		if len(srcParts) > 1 {
			srcKey, _ = url.PathUnescape(srcParts[1])
		} else {
			srcKey, _ = url.PathUnescape(srcParts[0])
		}

		srcObj, ok := s.objects[srcKey]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`))
			return
		}

		dataCopy := make([]byte, len(srcObj.data))
		copy(dataCopy, srcObj.data)
		s.objects[key] = &mockS3Object{
			data:        dataCopy,
			contentType: srcObj.contentType,
			etag:        srcObj.etag,
			modTime:     time.Now().UTC(),
			metadata:    srcObj.metadata,
		}

		respXML := fmt.Sprintf(`<CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ETag>%s</ETag><LastModified>%s</LastModified></CopyObjectResult>`, srcObj.etag, time.Now().UTC().Format(time.RFC3339))
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(respXML))
		return
	}

	// 8. PutObject: PUT /{bucket}/{key}
	if r.Method == http.MethodPut {
		body, _ := io.ReadAll(r.Body)
		h := md5.Sum(body)
		etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(h[:]))

		meta := make(map[string]string)
		for k, v := range r.Header {
			lower := strings.ToLower(k)
			if strings.HasPrefix(lower, "x-amz-meta-") {
				meta[strings.TrimPrefix(lower, "x-amz-meta-")] = strings.Join(v, ",")
			}
		}

		ct := r.Header.Get("Content-Type")
		if ct == "" {
			ct = "application/octet-stream"
		}

		s.objects[key] = &mockS3Object{
			data:        body,
			contentType: ct,
			etag:        etag,
			modTime:     time.Now().UTC(),
			metadata:    meta,
		}

		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusOK)
		return
	}

	// 9. ListObjectsV2: GET /{bucket}?list-type=2 or GET /{bucket}
	if r.Method == http.MethodGet && key == "" {
		prefix := r.URL.Query().Get("prefix")
		delimiter := r.URL.Query().Get("delimiter")

		var matchingKeys []string
		commonPrefixesMap := make(map[string]bool)

		for k := range s.objects {
			if strings.HasPrefix(k, prefix) {
				if delimiter != "" {
					afterPrefix := strings.TrimPrefix(k, prefix)
					idx := strings.Index(afterPrefix, delimiter)
					if idx >= 0 {
						cp := prefix + afterPrefix[:idx+len(delimiter)]
						commonPrefixesMap[cp] = true
						continue
					}
				}
				matchingKeys = append(matchingKeys, k)
			}
		}
		sort.Strings(matchingKeys)

		var xmlBuf bytes.Buffer
		xmlBuf.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		xmlBuf.WriteString(fmt.Sprintf(`<Name>%s</Name><Prefix>%s</Prefix>`, s.bucket, prefix))
		for _, k := range matchingKeys {
			obj := s.objects[k]
			xmlBuf.WriteString(fmt.Sprintf(`<Contents><Key>%s</Key><Size>%d</Size><ETag>%s</ETag><LastModified>%s</LastModified></Contents>`, k, len(obj.data), obj.etag, obj.modTime.Format(time.RFC3339)))
		}
		for cp := range commonPrefixesMap {
			xmlBuf.WriteString(fmt.Sprintf(`<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>`, cp))
		}
		xmlBuf.WriteString(`</ListBucketResult>`)

		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(xmlBuf.Bytes())
		return
	}

	// 10. GetObject / HeadObject
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		obj, ok := s.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			if r.Method == http.MethodGet {
				_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`))
			}
			return
		}

		// Preconditions check
		if err := blobkit.CheckPreconditions(obj.etag, obj.modTime, blobkit.GetOptions{
			IfMatch:           r.Header.Get("If-Match"),
			IfNoneMatch:       r.Header.Get("If-None-Match"),
			IfModifiedSince:   parseHTTPTime(r.Header.Get("If-Modified-Since")),
			IfUnmodifiedSince: parseHTTPTime(r.Header.Get("If-Unmodified-Since")),
		}); err != nil {
			if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
				w.WriteHeader(http.StatusNotModified)
			} else {
				w.WriteHeader(http.StatusPreconditionFailed)
				if r.Method == http.MethodGet {
					_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
				}
			}
			return
		}

		w.Header().Set("Content-Type", obj.contentType)
		w.Header().Set("ETag", obj.etag)
		w.Header().Set("Last-Modified", obj.modTime.Format(http.TimeFormat))
		for k, v := range obj.metadata {
			w.Header().Set(fmt.Sprintf("x-amz-meta-%s", k), v)
		}

		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" && strings.HasPrefix(rangeHeader, "bytes=") {
			parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
			start, _ := strconv.ParseInt(parts[0], 10, 64)
			var end int64 = int64(len(obj.data) - 1)
			if len(parts) > 1 && parts[1] != "" {
				end, _ = strconv.ParseInt(parts[1], 10, 64)
			}
			if start < 0 || start > int64(len(obj.data)) || end < start {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if end >= int64(len(obj.data)) {
				end = int64(len(obj.data) - 1)
			}
			chunk := obj.data[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.data)))
			w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
			w.WriteHeader(http.StatusPartialContent)
			if r.Method == http.MethodGet {
				_, _ = w.Write(chunk)
			}
			return
		}

		w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write(obj.data)
		}
		return
	}

	// 11. DeleteObject: DELETE /{bucket}/{key}
	if r.Method == http.MethodDelete {
		delete(s.objects, key)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.WriteHeader(http.StatusNotFound)
}

func parseHTTPTime(s string) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(http.TimeFormat, s)
	if err != nil {
		return nil
	}
	utc := t.UTC()
	return &utc
}

func setupTestS3Driver(t *testing.T, bucket string) (blobkit.Driver, func()) {
	t.Helper()
	mockSrv := newMockS3Server(bucket)
	ts := httptest.NewServer(mockSrv)

	cfg := s3.Config{
		Name:            "s3-test",
		Bucket:          bucket,
		Endpoint:        ts.URL,
		Region:          "us-east-1",
		AccessKeyID:     "MOCKKEY",
		SecretAccessKey: "MOCKSECRET",
		UsePathStyle:    true,
		HTTPClient:      ts.Client(),
		MaxRetries:      1,
	}

	driver, err := s3.NewDriver(cfg)
	if err != nil {
		ts.Close()
		t.Fatalf("failed to create s3 driver: %v", err)
	}

	cleanup := func() {
		_ = driver.Close()
		ts.Close()
	}

	return driver, cleanup
}

func TestS3Driver_Contract(t *testing.T) {
	testutil.RunDriverContractTests(t, func(t *testing.T) (blobkit.Driver, func()) {
		return setupTestS3Driver(t, "test-contract-bucket")
	})
}
