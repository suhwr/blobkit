package azure

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/suhwr/blobkit"
)

type blockListXML struct {
	XMLName xml.Name `xml:"BlockList"`
	Latest  []string `xml:"Latest"`
}

type azureBlockListResponse struct {
	XMLName           xml.Name         `xml:"BlockList"`
	CommittedBlocks   []azureBlockItem `xml:"CommittedBlocks>Block"`
	UncommittedBlocks []azureBlockItem `xml:"UncommittedBlocks>Block"`
}

type azureBlockItem struct {
	Name string `xml:"Name"`
	Size int64  `xml:"Size"`
}

// formatBlockID produces a deterministic, base64-encoded, fixed-width block identifier.
func formatBlockID(partNumber int32) string {
	raw := fmt.Sprintf("%08d", partNumber)
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

// parseBlockID decodes a base64 block ID back into a part number.
func parseBlockID(blockID string) int32 {
	data, err := base64.StdEncoding.DecodeString(blockID)
	if err != nil {
		return 0
	}
	num, err := strconv.Atoi(string(data))
	if err != nil {
		return 0
	}
	return int32(num)
}

// CreateMultipart initiates a Block Blob multipart session.
func (d *Driver) CreateMultipart(ctx context.Context, obj *blobkit.Object, opts blobkit.PutOptions) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", blobkit.WrapError("create_multipart", "", d.cfg.Name, err)
	}
	if obj == nil || obj.Key == "" {
		return "", blobkit.ErrInvalidKey
	}
	if err := validateKey(obj.Key); err != nil {
		return "", blobkit.WrapError("create_multipart", obj.Key, d.cfg.Name, err)
	}

	uploadID := uuid.New().String()
	d.sessionsMu.Lock()
	d.sessions[uploadID] = obj.Key
	d.sessionsMu.Unlock()

	return uploadID, nil
}

// UploadPart uploads a single chunk as a staged block (PUT ?comp=block&blockid=...).
func (d *Driver) UploadPart(ctx context.Context, key string, uploadID string, partNumber int32, r io.Reader, size int64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, err)
	}
	if r == nil {
		return "", blobkit.ErrNilReader
	}
	if err := validateKey(key); err != nil {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, err)
	}
	if partNumber <= 0 {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, fmt.Errorf("part number must be >= 1"))
	}

	d.sessionsMu.RLock()
	expectedKey, ok := d.sessions[uploadID]
	d.sessionsMu.RUnlock()
	if !ok || expectedKey != key {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, blobkit.ErrSessionNotFound)
	}

	blockID := formatBlockID(partNumber)
	urlStr := fmt.Sprintf("%s?comp=block&blockid=%s", d.blobURL(key), blockID)

	// Buffer or limit read
	var bodyReader io.Reader = r
	if size > 0 {
		bodyReader = io.LimitReader(r, size)
	}

	buf, err := io.ReadAll(bodyReader)
	if err != nil {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, err)
	}
	if size > 0 && int64(len(buf)) != size {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, blobkit.ErrSizeMismatch)
	}

	h := md5.Sum(buf)
	md5Hex := hex.EncodeToString(h[:])
	md5B64 := base64.StdEncoding.EncodeToString(h[:])

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, urlStr, bytes.NewReader(buf))
	if err != nil {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, err)
	}

	req.ContentLength = int64(len(buf))
	req.Header.Set("Content-MD5", md5B64)

	if err := d.authorize(req); err != nil {
		return "", blobkit.WrapError("upload_part", key, d.cfg.Name, err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return "", wrapHTTPError("upload_part", key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		bodyBytes := readErrorBody(resp.Body)
		return "", wrapHTTPError("upload_part", key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	etag := fmt.Sprintf("\"%s\"", md5Hex)
	return etag, nil
}

// CompleteMultipart commits the staged blocks into the final Block Blob (PUT ?comp=blocklist).
func (d *Driver) CompleteMultipart(ctx context.Context, obj *blobkit.Object, uploadID string, parts []blobkit.CompletedPart) (*blobkit.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("complete_multipart", "", d.cfg.Name, err)
	}
	if obj == nil || obj.Key == "" {
		return nil, blobkit.ErrInvalidKey
	}
	if err := validateKey(obj.Key); err != nil {
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, err)
	}

	d.sessionsMu.Lock()
	expectedKey, ok := d.sessions[uploadID]
	if ok {
		delete(d.sessions, uploadID)
	}
	d.sessionsMu.Unlock()
	if !ok || expectedKey != obj.Key {
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, blobkit.ErrSessionNotFound)
	}
	if len(parts) == 0 {
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, fmt.Errorf("no parts provided"))
	}

	// Sort parts by PartNumber
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	bl := blockListXML{
		Latest: make([]string, len(parts)),
	}
	var totalSize int64
	for i, p := range parts {
		bl.Latest[i] = formatBlockID(p.PartNumber)
		totalSize += p.Size
	}

	xmlData, err := xml.Marshal(bl)
	if err != nil {
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, err)
	}

	urlStr := fmt.Sprintf("%s?comp=blocklist", d.blobURL(obj.Key))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, urlStr, bytes.NewReader(xmlData))
	if err != nil {
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, err)
	}

	req.ContentLength = int64(len(xmlData))
	req.Header.Set("Content-Type", "application/xml; charset=utf-8")
	if obj.ContentType != "" {
		req.Header.Set("x-ms-blob-content-type", obj.ContentType)
	}

	// Set user metadata
	for k, v := range obj.Metadata {
		req.Header.Set(fmt.Sprintf("x-ms-meta-%s", k), v)
	}

	if err := d.authorize(req); err != nil {
		return nil, blobkit.WrapError("complete_multipart", obj.Key, d.cfg.Name, err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, wrapHTTPError("complete_multipart", obj.Key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		bodyBytes := readErrorBody(resp.Body)
		return nil, wrapHTTPError("complete_multipart", obj.Key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	now := time.Now().UTC()
	etag := resp.Header.Get("ETag")

	res := *obj
	res.Bucket = d.cfg.Container
	res.Size = totalSize
	res.ETag = etag
	res.UpdatedAt = now
	if res.CreatedAt.IsZero() {
		res.CreatedAt = now
	}
	res.Provider = d.cfg.Name
	res.Status = blobkit.StateCommitted

	return &res, nil
}

// AbortMultipart unregisters the active multipart session.
func (d *Driver) AbortMultipart(ctx context.Context, key string, uploadID string) error {
	if err := ctx.Err(); err != nil {
		return blobkit.WrapError("abort_multipart", key, d.cfg.Name, err)
	}
	if err := validateKey(key); err != nil {
		return blobkit.WrapError("abort_multipart", key, d.cfg.Name, err)
	}
	d.sessionsMu.Lock()
	expectedKey, ok := d.sessions[uploadID]
	if ok {
		delete(d.sessions, uploadID)
	}
	d.sessionsMu.Unlock()
	if !ok || expectedKey != key {
		return blobkit.WrapError("abort_multipart", key, d.cfg.Name, blobkit.ErrSessionNotFound)
	}
	return nil
}

// ListParts queries staged uncommitted and committed blocks for the blob (GET ?comp=blocklist&blocklisttype=all).
func (d *Driver) ListParts(ctx context.Context, key string, uploadID string) ([]blobkit.CompletedPart, error) {
	if err := ctx.Err(); err != nil {
		return nil, blobkit.WrapError("list_parts", key, d.cfg.Name, err)
	}
	if err := validateKey(key); err != nil {
		return nil, blobkit.WrapError("list_parts", key, d.cfg.Name, err)
	}
	d.sessionsMu.RLock()
	expectedKey, ok := d.sessions[uploadID]
	d.sessionsMu.RUnlock()
	if !ok || expectedKey != key {
		return nil, blobkit.WrapError("list_parts", key, d.cfg.Name, blobkit.ErrSessionNotFound)
	}

	urlStr := fmt.Sprintf("%s?comp=blocklist&blocklisttype=all", d.blobURL(key))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, blobkit.WrapError("list_parts", key, d.cfg.Name, err)
	}

	if err := d.authorize(req); err != nil {
		return nil, blobkit.WrapError("list_parts", key, d.cfg.Name, err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, wrapHTTPError("list_parts", key, d.cfg.Name, 0, nil, err)
	}
	defer resp.Body.Close()

	bodyBytes := readErrorBody(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, wrapHTTPError("list_parts", key, d.cfg.Name, resp.StatusCode, bodyBytes, nil)
	}

	var blResp azureBlockListResponse
	if err := xml.Unmarshal(bodyBytes, &blResp); err != nil {
		return nil, blobkit.WrapError("list_parts", key, d.cfg.Name, err)
	}

	var parts []blobkit.CompletedPart
	for _, b := range append(blResp.CommittedBlocks, blResp.UncommittedBlocks...) {
		num := parseBlockID(b.Name)
		if num > 0 {
			parts = append(parts, blobkit.CompletedPart{
				PartNumber: num,
				Size:       b.Size,
			})
		}
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	return parts, nil
}
