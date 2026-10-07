package blobkit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/suhwr/blobkit/key"
	"github.com/suhwr/blobkit/mime"
)

// Client is the high-level object storage infrastructure orchestrator.
// It sits between application logic and physical storage drivers, managing
// identity, key generation, streaming I/O, routing, metadata, and access delivery.
type Client struct {
	router            Router
	keyGen            key.Generator
	urlResolver       URLResolver
	registry          MetadataStore
	defaultVisibility Visibility
	policy            *Policy
	observer          Observer
	cache             Cache

	sessionsMu sync.RWMutex
	sessions   map[string]*UploadSession
}

// New constructs a new BlobKit Client configured with the provided options.
func New(options ...Option) (*Client, error) {
	c := &Client{
		keyGen:            key.NewUUIDv7Generator(),
		defaultVisibility: VisibilityPrivate,
		observer:          NoopObserver{},
		sessions:          make(map[string]*UploadSession),
	}

	for _, opt := range options {
		opt(c)
	}

	if c.observer == nil {
		c.observer = NoopObserver{}
	}

	if c.router == nil {
		return nil, fmt.Errorf("%w: at least one driver or router must be configured", ErrProviderUnavailable)
	}

	return c, nil
}

// Put uploads an object stream based on the caller's semantic intent.
// It assigns a canonical logical ObjectID, generates a physical storage key,
// peeks MIME types without buffering entire files, and commits metadata to the registry if enabled.
func (c *Client) Put(ctx context.Context, r io.Reader, opts PutOptions) (savedObj *Object, err error) {
	start := time.Now()
	ctx = c.observer.OnOperationStart(ctx, OpPut, opts.Filename)
	defer func() {
		c.observer.OnOperationEnd(ctx, OpPut, opts.Filename, time.Since(start), err)
	}()

	if r == nil {
		err = ErrNilReader
		return nil, err
	}

	// 1. Assign or generate canonical logical ObjectID (UUIDv7)
	objID := opts.ID
	if objID == "" {
		// New upload: auto-generate candidate ID with collision retry loop
		for attempt := 0; attempt < 5; attempt++ {
			candidateID, genErr := key.NewObjectID()
			if genErr != nil {
				return nil, WrapError("put_generate_id", "", "", genErr)
			}
			objID = candidateID
			if c.registry != nil {
				existing, getErr := c.registry.GetByID(ctx, objID)
				if getErr == nil && existing != nil {
					// Extremely rare UUIDv7 collision: regenerate new ID
					continue
				}
			}
			break
		}
	} else if c.registry != nil {
		// Intentional update of existing object: verify object is not locked
		existing, getErr := c.registry.GetByID(ctx, objID)
		if getErr == nil && existing != nil {
			if existing.LegalHold {
				return nil, WrapError("put_overwrite", existing.Key, existing.Provider, ErrObjectLocked)
			}
			if existing.RetentionUntil != nil && existing.RetentionUntil.After(time.Now().UTC()) {
				return nil, WrapError("put_overwrite", existing.Key, existing.Provider, ErrObjectLocked)
			}
		}
	}

	// 2. Peek MIME type without consuming the stream into memory
	reconstructedReader, detectedMIME, err := mime.Sniff(r, opts.Filename, opts.ContentType)
	if err != nil {
		return nil, WrapError("sniff_mime", "", "", err)
	}

	// 3. Apply security policy and sanitization
	cleanFilename := SanitizeFilename(opts.Filename)
	opts.Filename = cleanFilename
	opts.ContentDisposition = SanitizeHeader(opts.ContentDisposition)
	opts.CacheControl = SanitizeHeader(opts.CacheControl)

	activePolicy := opts.Policy
	if activePolicy == nil {
		activePolicy = c.policy
	}
	if activePolicy != nil {
		if err := activePolicy.Validate(ctx, ValidationInput{
			Namespace:    opts.Namespace,
			Filename:     opts.Filename,
			ContentType:  detectedMIME,
			Size:         opts.Size,
			StreamReader: reconstructedReader,
		}); err != nil {
			return nil, WrapError("policy_validation", "", "", err)
		}
	}

	// 4. Formulate physical storage key
	storageKey := opts.Key
	if storageKey == "" {
		var genErr error
		storageKey, genErr = c.keyGen.Generate(ctx, key.KeyInput{
			ID:        objID,
			Namespace: opts.Namespace,
			Filename:  opts.Filename,
			Ext:       key.NormalizeExtension(opts.Filename, ""),
			CreatedAt: time.Now().UTC(),
		})
		if genErr != nil {
			return nil, WrapError("generate_key", "", "", genErr)
		}
	}

	vis := opts.Visibility
	if vis == "" {
		vis = c.defaultVisibility
	}

	obj := &Object{
		ID:               objID,
		Namespace:        opts.Namespace,
		OwnerID:          opts.OwnerID,
		Key:              storageKey,
		ContentType:      detectedMIME,
		OriginalFilename: opts.Filename,
		Visibility:       vis,
		Status:           StatePending,
		Metadata:         opts.Metadata,
		RetentionUntil:   opts.RetentionUntil,
		ExpiresAt:        opts.ExpiresAt,
		LegalHold:        opts.LegalHold,
		ClientChecksum:   opts.ClientChecksum,
		CreatedAt:        time.Now().UTC(),
	}

	// 5. Record initial pending state in database registry if enabled
	if c.registry != nil {
		rec := &Record{
			ObjectID:         obj.ID,
			Namespace:        obj.Namespace,
			OwnerID:          obj.OwnerID,
			Key:              obj.Key,
			MIMEType:         obj.ContentType,
			OriginalFilename: obj.OriginalFilename,
			Visibility:       obj.Visibility,
			Status:           StatePending,
			Metadata:         obj.Metadata,
			RetentionUntil:   obj.RetentionUntil,
			ExpiresAt:        obj.ExpiresAt,
			LegalHold:        obj.LegalHold,
			ClientChecksum:   obj.ClientChecksum,
			CreatedAt:        obj.CreatedAt,
		}
		if saveErr := c.registry.Save(ctx, rec); saveErr != nil {
			return nil, WrapError("registry_save_pending", obj.Key, "", saveErr)
		}
	}

	// 6. Select driver from router
	driver, err := c.router.Select(ctx, RouteContext{
		Op:             OpPut,
		Key:            storageKey,
		Namespace:      opts.Namespace,
		ForcedProvider: opts.Provider,
	})
	if err != nil {
		c.abortRegistry(ctx, obj.ID)
		return nil, WrapError("route_driver", storageKey, "", err)
	}

	// 7. Calculate streaming SHA-256 via io.TeeReader without RAM bloat
	hasher := sha256.New()
	uploadStream := io.TeeReader(reconstructedReader, hasher)

	// 8. Perform physical upload
	savedObj, uploadErr := driver.Put(ctx, obj, uploadStream, opts)
	if uploadErr != nil {
		c.router.ReportFailure(driver.Name(), uploadErr)
		c.abortRegistry(ctx, obj.ID)
		return nil, uploadErr
	}
	c.router.ReportSuccess(driver.Name())

	calculatedHash := hex.EncodeToString(hasher.Sum(nil))

	// Verify client-declared checksum if specified
	if opts.ClientChecksum != "" && !strings.EqualFold(calculatedHash, opts.ClientChecksum) {
		_ = driver.Delete(ctx, savedObj.Key)
		c.abortRegistry(ctx, obj.ID)
		return nil, WrapError("checksum_verify", savedObj.Key, driver.Name(), ErrChecksumMismatch)
	}

	savedObj.ChecksumSHA256 = calculatedHash
	savedObj.ClientChecksum = opts.ClientChecksum
	savedObj.RetentionUntil = opts.RetentionUntil
	savedObj.ExpiresAt = opts.ExpiresAt
	savedObj.LegalHold = opts.LegalHold
	savedObj.Status = StateCommitted

	// 9. Commit metadata record in database registry
	if c.registry != nil {
		rec := &Record{
			ObjectID:         savedObj.ID,
			Namespace:        savedObj.Namespace,
			OwnerID:          savedObj.OwnerID,
			Key:              savedObj.Key,
			Bucket:           savedObj.Bucket,
			Provider:         savedObj.Provider,
			MIMEType:         savedObj.ContentType,
			Size:             savedObj.Size,
			ChecksumSHA256:   savedObj.ChecksumSHA256,
			OriginalFilename: savedObj.OriginalFilename,
			Visibility:       savedObj.Visibility,
			Status:           StateCommitted,
			Metadata:         savedObj.Metadata,
			RetentionUntil:   savedObj.RetentionUntil,
			ExpiresAt:        savedObj.ExpiresAt,
			LegalHold:        savedObj.LegalHold,
			ClientChecksum:   savedObj.ClientChecksum,
			CreatedAt:        savedObj.CreatedAt,
			UpdatedAt:        savedObj.UpdatedAt,
		}
		if err = c.registry.Save(ctx, rec); err != nil {
			return savedObj, WrapError("registry_save_committed", savedObj.Key, savedObj.Provider, err)
		}
	}

	if c.cache != nil {
		c.cache.Delete(savedObj.Key)
		c.cache.Delete(savedObj.ID)
	}
	c.observer.OnBytesTransferred(OpPut, savedObj.Size)

	return savedObj, nil
}

// Get retrieves an object stream. Target can be either a canonical logical ObjectID
// (when registry is enabled) or a physical storage key.
func (c *Client) Get(ctx context.Context, target string, opts GetOptions) (reader *ObjectReader, err error) {
	start := time.Now()
	ctx = c.observer.OnOperationStart(ctx, OpGet, target)
	defer func() {
		c.observer.OnOperationEnd(ctx, OpGet, target, time.Since(start), err)
	}()

	key, providerName, resErr := c.resolveTarget(ctx, target)
	if resErr != nil {
		err = resErr
		return nil, err
	}

	driver, rErr := c.router.Select(ctx, RouteContext{
		Op:             OpGet,
		Key:            key,
		ForcedProvider: providerName,
	})
	if rErr != nil {
		err = WrapError("route_driver", key, "", rErr)
		return nil, err
	}

	reader, getErr := driver.Get(ctx, key, opts)
	if getErr != nil {
		c.router.ReportFailure(driver.Name(), getErr)
		err = getErr
		return nil, err
	}
	c.router.ReportSuccess(driver.Name())

	// Augment with logical ID if known
	if target != key {
		reader.ID = target
	}

	return reader, nil
}

// Head inspects an object's metadata without reading its payload.
func (c *Client) Head(ctx context.Context, target string) (obj *Object, err error) {
	start := time.Now()
	ctx = c.observer.OnOperationStart(ctx, OpHead, target)
	defer func() {
		c.observer.OnOperationEnd(ctx, OpHead, target, time.Since(start), err)
	}()

	if c.cache != nil {
		if c.cache.IsNegative(target) {
			err = WrapError("head", target, "", ErrObjectNotFound)
			return nil, err
		}
		if cached, ok := c.cache.Get(target); ok {
			return cached, nil
		}
	}

	key, providerName, resErr := c.resolveTarget(ctx, target)
	if resErr != nil {
		err = resErr
		if c.cache != nil && errors.Is(err, ErrObjectNotFound) {
			c.cache.SetNegative(target, 30*time.Second)
		}
		return nil, err
	}

	driver, rErr := c.router.Select(ctx, RouteContext{
		Op:             OpHead,
		Key:            key,
		ForcedProvider: providerName,
	})
	if rErr != nil {
		err = WrapError("route_driver", key, "", rErr)
		return nil, err
	}

	obj, headErr := driver.Head(ctx, key)
	if headErr != nil {
		c.router.ReportFailure(driver.Name(), headErr)
		err = headErr
		if c.cache != nil && errors.Is(err, ErrObjectNotFound) {
			c.cache.SetNegative(target, 30*time.Second)
			c.cache.SetNegative(key, 30*time.Second)
		}
		return nil, err
	}
	c.router.ReportSuccess(driver.Name())

	if target != key {
		obj.ID = target
	}

	if c.registry != nil {
		rec, _ := c.registry.GetByID(ctx, target)
		if rec == nil {
			rec, _ = c.registry.GetByKey(ctx, key)
		}
		if rec != nil {
			obj.Status = rec.Status
			obj.Visibility = rec.Visibility
			obj.OriginalFilename = rec.OriginalFilename
			obj.RetentionUntil = rec.RetentionUntil
			obj.ExpiresAt = rec.ExpiresAt
			obj.LegalHold = rec.LegalHold
			obj.Metadata = rec.Metadata
			obj.DeletedAt = rec.DeletedAt
		}
	}

	if c.cache != nil {
		c.cache.Set(target, obj, 5*time.Minute)
		if target != key {
			c.cache.Set(key, obj, 5*time.Minute)
		}
	}

	return obj, nil
}

// Delete removes an object from storage and marks or deletes it in the registry.
func (c *Client) Delete(ctx context.Context, target string) (err error) {
	start := time.Now()
	ctx = c.observer.OnOperationStart(ctx, OpDelete, target)
	defer func() {
		c.observer.OnOperationEnd(ctx, OpDelete, target, time.Since(start), err)
	}()

	key, providerName, resErr := c.resolveTarget(ctx, target)
	if resErr != nil {
		err = resErr
		return err
	}

	if c.registry != nil {
		rec, _ := c.registry.GetByID(ctx, target)
		if rec == nil {
			rec, _ = c.registry.GetByKey(ctx, key)
		}
		if rec != nil {
			if rec.LegalHold {
				err = WrapError("delete", key, providerName, ErrObjectLocked)
				return err
			}
			if rec.RetentionUntil != nil && rec.RetentionUntil.After(time.Now().UTC()) {
				err = WrapError("delete", key, providerName, ErrObjectLocked)
				return err
			}
		}
	}

	driver, rErr := c.router.Select(ctx, RouteContext{
		Op:             OpDelete,
		Key:            key,
		ForcedProvider: providerName,
	})
	if rErr != nil {
		err = WrapError("route_driver", key, "", rErr)
		return err
	}

	if delErr := driver.Delete(ctx, key); delErr != nil {
		c.router.ReportFailure(driver.Name(), delErr)
		err = delErr
		return err
	}
	c.router.ReportSuccess(driver.Name())

	if c.registry != nil {
		_ = c.registry.UpdateStatus(ctx, target, StateDeleted)
	}

	if c.cache != nil {
		c.cache.Delete(target)
		c.cache.Delete(key)
	}

	return nil
}

// DeleteBatch removes multiple objects in a single batch operation.
func (c *Client) DeleteBatch(ctx context.Context, targets []string) (deleted []string, err error) {
	if len(targets) == 0 {
		return nil, nil
	}

	start := time.Now()
	ctx = c.observer.OnOperationStart(ctx, OpDelete, fmt.Sprintf("batch:%d", len(targets)))
	defer func() {
		c.observer.OnOperationEnd(ctx, OpDelete, fmt.Sprintf("batch:%d", len(targets)), time.Since(start), err)
	}()

	driver, rErr := c.router.Select(ctx, RouteContext{Op: OpDelete})
	if rErr != nil {
		err = WrapError("route_driver", "", "", rErr)
		return nil, err
	}

	// Resolve any ObjectIDs to physical keys
	resolvedKeys := make([]string, len(targets))
	for i, t := range targets {
		k, _, _ := c.resolveTarget(ctx, t)
		resolvedKeys[i] = k
	}

	deleted, err = driver.DeleteBatch(ctx, resolvedKeys)
	if err != nil {
		c.router.ReportFailure(driver.Name(), err)
		return deleted, err
	}
	c.router.ReportSuccess(driver.Name())

	if c.registry != nil {
		for _, t := range targets {
			_ = c.registry.UpdateStatus(ctx, t, StateDeleted)
		}
	}

	if c.cache != nil {
		for _, t := range targets {
			c.cache.Delete(t)
		}
		for _, k := range resolvedKeys {
			c.cache.Delete(k)
		}
	}

	return deleted, nil
}

// Exists reports whether an object exists in storage without downloading its body.
func (c *Client) Exists(ctx context.Context, target string) (bool, error) {
	_, err := c.Head(ctx, target)
	if err == nil {
		return true, nil
	}
	if IsNotFound(err) {
		return false, nil
	}
	return false, err
}

// Rename updates the logical client-facing filename without moving or re-uploading physical bytes.
func (c *Client) Rename(ctx context.Context, target string, newFilename string) error {
	if newFilename == "" {
		return ErrInvalidFilename
	}
	clean := SanitizeFilename(newFilename)
	if clean == "" {
		return ErrInvalidFilename
	}

	if c.registry != nil {
		if err := c.registry.UpdateFilename(ctx, target, clean); err != nil {
			return WrapError("rename", target, "", err)
		}
		return nil
	}
	return ErrUnsupportedOperation
}

// UpdateMetadata replaces or updates user-defined metadata attributes for an object.
func (c *Client) UpdateMetadata(ctx context.Context, target string, metadata map[string]string) error {
	if c.registry == nil {
		return ErrUnsupportedOperation
	}
	objID := target
	cleanKey := strings.TrimLeft(target, "/")
	if rec, err := c.registry.GetByKey(ctx, cleanKey); err == nil && rec != nil {
		objID = rec.ObjectID
	}
	if err := c.registry.UpdateMetadata(ctx, objID, metadata); err != nil {
		return WrapError("update_metadata", objID, "", err)
	}
	return nil
}

// BatchUpdateMetadata applies metadata key-value updates across multiple objects in the registry.
func (c *Client) BatchUpdateMetadata(ctx context.Context, updates map[string]map[string]string) error {
	if c.registry == nil {
		return ErrUnsupportedOperation
	}
	for id, meta := range updates {
		if err := c.registry.UpdateMetadata(ctx, id, meta); err != nil {
			return WrapError("batch_update_metadata", id, "", err)
		}
	}
	return nil
}

// Copy duplicates an object to a new logical destination.
// If the source and destination drivers are identical and support CapCopy,
// a fast server-side copy is executed; otherwise, data is streamed between drivers.
func (c *Client) Copy(ctx context.Context, srcTarget string, dstOpts PutOptions) (*Object, error) {
	srcKey, srcProvider, err := c.resolveTarget(ctx, srcTarget)
	if err != nil {
		return nil, err
	}

	srcObj, err := c.Head(ctx, srcTarget)
	if err != nil {
		return nil, WrapError("copy_source_head", srcKey, srcProvider, err)
	}

	dstID := dstOpts.ID
	if dstID == "" {
		for attempt := 0; attempt < 5; attempt++ {
			candidateID, genErr := key.NewObjectID()
			if genErr != nil {
				return nil, WrapError("copy_generate_id", "", "", genErr)
			}
			dstID = candidateID
			if c.registry != nil {
				existing, getErr := c.registry.GetByID(ctx, dstID)
				if getErr == nil && existing != nil {
					continue
				}
			}
			break
		}
	}

	dstKey := dstOpts.Key
	if dstKey == "" {
		filename := dstOpts.Filename
		if filename == "" {
			filename = srcObj.OriginalFilename
		}
		dstKey, err = c.keyGen.Generate(ctx, key.KeyInput{
			ID:        dstID,
			Namespace: dstOpts.Namespace,
			Filename:  filename,
			Ext:       key.NormalizeExtension(filename, ""),
			CreatedAt: time.Now().UTC(),
		})
		if err != nil {
			return nil, WrapError("copy_dst_key", "", "", err)
		}
	}

	dstDriver, err := c.router.Select(ctx, RouteContext{
		Op:             OpPut,
		Key:            dstKey,
		Namespace:      dstOpts.Namespace,
		ForcedProvider: dstOpts.Provider,
	})
	if err != nil {
		return nil, WrapError("route_driver", dstKey, "", err)
	}

	// Server-side copy if same driver and CapCopy is supported
	if (srcProvider == "" || srcProvider == dstDriver.Name()) && (dstDriver.Capabilities()&CapCopy != 0) {
		if err := dstDriver.Copy(ctx, srcKey, dstKey); err != nil {
			return nil, err
		}
		copiedObj, err := dstDriver.Head(ctx, dstKey)
		if err != nil {
			return nil, err
		}
		copiedObj.ID = dstID
		copiedObj.Namespace = dstOpts.Namespace
		copiedObj.OwnerID = dstOpts.OwnerID
		copiedObj.OriginalFilename = dstOpts.Filename
		if copiedObj.OriginalFilename == "" {
			copiedObj.OriginalFilename = srcObj.OriginalFilename
		}
		copiedObj.Status = StateCommitted

		if c.registry != nil {
			rec := &Record{
				ObjectID:         copiedObj.ID,
				Namespace:        copiedObj.Namespace,
				OwnerID:          copiedObj.OwnerID,
				Key:              copiedObj.Key,
				Bucket:           copiedObj.Bucket,
				Provider:         copiedObj.Provider,
				MIMEType:         copiedObj.ContentType,
				Size:             copiedObj.Size,
				ChecksumSHA256:   copiedObj.ChecksumSHA256,
				OriginalFilename: copiedObj.OriginalFilename,
				Visibility:       copiedObj.Visibility,
				Status:           StateCommitted,
				Metadata:         dstOpts.Metadata,
				CreatedAt:        time.Now().UTC(),
				UpdatedAt:        time.Now().UTC(),
			}
			if err := c.registry.Save(ctx, rec); err != nil {
				return copiedObj, WrapError("registry_save_copy", copiedObj.Key, copiedObj.Provider, err)
			}
		}
		return copiedObj, nil
	}

	// Cross-driver streaming copy fallback
	srcReader, err := c.Get(ctx, srcTarget, GetOptions{})
	if err != nil {
		return nil, WrapError("copy_get_stream", srcKey, srcProvider, err)
	}
	defer srcReader.Close()

	if dstOpts.Filename == "" {
		dstOpts.Filename = srcObj.OriginalFilename
	}
	if dstOpts.ContentType == "" {
		dstOpts.ContentType = srcObj.ContentType
	}
	dstOpts.ID = dstID
	dstOpts.Key = dstKey

	return c.Put(ctx, srcReader, dstOpts)
}

// Move copies an object to a new destination and deletes the original.
func (c *Client) Move(ctx context.Context, srcTarget string, dstOpts PutOptions) (*Object, error) {
	copied, err := c.Copy(ctx, srcTarget, dstOpts)
	if err != nil {
		return nil, WrapError("move_copy_phase", "", "", err)
	}

	if err := c.Delete(ctx, srcTarget); err != nil {
		return copied, WrapError("move_delete_phase", "", "", err)
	}

	return copied, nil
}

// List lists objects directly from the storage driver.
func (c *Client) List(ctx context.Context, opts ListOptions) (*ListResult, error) {
	driver, err := c.router.Select(ctx, RouteContext{Op: OpList})
	if err != nil {
		return nil, WrapError("route_driver", "", "", err)
	}

	res, err := driver.List(ctx, opts)
	if err != nil {
		c.router.ReportFailure(driver.Name(), err)
		return nil, err
	}
	c.router.ReportSuccess(driver.Name())

	return res, nil
}

// Find queries objects by metadata attributes (namespace, owner_id, status, mime, etc.)
// from the database registry without requiring knowledge of storage keys or URLs.
func (c *Client) Find(ctx context.Context, filter Filter) ([]Object, error) {
	if c.registry == nil {
		return nil, fmt.Errorf("%w: Find requires an enabled metadata registry", ErrUnsupportedOperation)
	}

	records, err := c.registry.Find(ctx, filter)
	if err != nil {
		return nil, WrapError("find", "", "", err)
	}

	objects := make([]Object, len(records))
	for i, r := range records {
		objects[i] = r.ToObject()
	}
	return objects, nil
}

// ResolveURL generates a public CDN or base delivery URL on demand.
// Target can be an ObjectID or a physical storage key.
func (c *Client) ResolveURL(ctx context.Context, target string) (string, error) {
	key, providerName, err := c.resolveTarget(ctx, target)
	if err != nil {
		return "", err
	}

	// 1. If explicit URLResolver is configured on Client, use it
	if c.urlResolver != nil {
		resolved := c.urlResolver.ResolveURL(key)
		if resolved != "" {
			return resolved, nil
		}
	}

	// 2. Delegate to the matching provider driver's resolution
	driver, err := c.router.Select(ctx, RouteContext{
		Op:             OpGet,
		Key:            key,
		ForcedProvider: providerName,
	})
	if err != nil {
		return "", WrapError("route_driver", key, "", err)
	}

	return driver.ResolveURL(key)
}

// PresignGet generates a time-limited signed URL for authorized download.
// Target can be an ObjectID or a physical storage key.
func (c *Client) PresignGet(ctx context.Context, target string, opts PresignOptions) (*PresignedURL, error) {
	key, providerName, err := c.resolveTarget(ctx, target)
	if err != nil {
		return nil, err
	}

	driver, err := c.router.Select(ctx, RouteContext{
		Op:             OpPresign,
		Key:            key,
		ForcedProvider: providerName,
	})
	if err != nil {
		return nil, WrapError("route_driver", key, "", err)
	}

	presigned, err := driver.PresignGet(ctx, key, opts)
	if err != nil {
		c.router.ReportFailure(driver.Name(), err)
		return nil, err
	}
	c.router.ReportSuccess(driver.Name())

	return presigned, nil
}

// PresignPut generates a time-limited signed URL for direct client upload.
// It assigns an ObjectID, calculates the physical storage key, and registers a pending record.
func (c *Client) PresignPut(ctx context.Context, opts PutOptions, presignOpts PresignOptions) (*PresignedURL, *Object, error) {
	objID := opts.ID
	if objID == "" {
		var err error
		objID, err = key.NewObjectID()
		if err != nil {
			return nil, nil, WrapError("presign_put", "", "", err)
		}
	}

	storageKey := opts.Key
	if storageKey == "" {
		var genErr error
		storageKey, genErr = c.keyGen.Generate(ctx, key.KeyInput{
			ID:        objID,
			Namespace: opts.Namespace,
			Filename:  opts.Filename,
			Ext:       key.NormalizeExtension(opts.Filename, ""),
			CreatedAt: time.Now().UTC(),
		})
		if genErr != nil {
			return nil, nil, WrapError("generate_key", "", "", genErr)
		}
	}

	vis := opts.Visibility
	if vis == "" {
		vis = c.defaultVisibility
	}

	obj := &Object{
		ID:               objID,
		Namespace:        opts.Namespace,
		OwnerID:          opts.OwnerID,
		Key:              storageKey,
		ContentType:      opts.ContentType,
		OriginalFilename: opts.Filename,
		Visibility:       vis,
		Status:           StatePending,
		Metadata:         opts.Metadata,
		CreatedAt:        time.Now().UTC(),
	}

	driver, err := c.router.Select(ctx, RouteContext{
		Op:             OpPresign,
		Key:            storageKey,
		Namespace:      opts.Namespace,
		ForcedProvider: opts.Provider,
	})
	if err != nil {
		return nil, nil, WrapError("route_driver", storageKey, "", err)
	}

	obj.Provider = driver.Name()

	// Register pending record
	if c.registry != nil {
		rec := &Record{
			ObjectID:         obj.ID,
			Namespace:        obj.Namespace,
			OwnerID:          obj.OwnerID,
			Key:              obj.Key,
			Provider:         obj.Provider,
			MIMEType:         obj.ContentType,
			OriginalFilename: obj.OriginalFilename,
			Visibility:       obj.Visibility,
			Status:           StatePending,
			Metadata:         obj.Metadata,
			CreatedAt:        obj.CreatedAt,
		}
		if saveErr := c.registry.Save(ctx, rec); saveErr != nil {
			return nil, nil, WrapError("registry_save_pending", storageKey, "", saveErr)
		}
	}

	presigned, err := driver.PresignPut(ctx, storageKey, presignOpts)
	if err != nil {
		c.router.ReportFailure(driver.Name(), err)
		c.abortRegistry(ctx, obj.ID)
		return nil, nil, err
	}
	c.router.ReportSuccess(driver.Name())

	return presigned, obj, nil
}

// Commit verifies that an uploaded blob exists in storage and transitions its status
// from StatePending to StateCommitted in the database registry.
func (c *Client) Commit(ctx context.Context, objectID string) error {
	if c.registry == nil {
		return nil
	}

	rec, err := c.registry.GetByID(ctx, objectID)
	if err != nil {
		return WrapError("commit", objectID, "", err)
	}

	driver, err := c.router.Select(ctx, RouteContext{
		Op:             OpHead,
		Key:            rec.Key,
		ForcedProvider: rec.Provider,
	})
	if err != nil {
		return WrapError("route_driver", rec.Key, "", err)
	}

	// Verify object exists in physical storage
	headObj, headErr := driver.Head(ctx, rec.Key)
	if headErr != nil {
		return WrapError("commit_verify", rec.Key, rec.Provider, headErr)
	}

	rec.Size = headObj.Size
	rec.Bucket = headObj.Bucket
	rec.Status = StateCommitted
	rec.UpdatedAt = time.Now().UTC()

	return c.registry.Save(ctx, rec)
}

// Close gracefully releases any connections across all registered drivers and the registry.
func (c *Client) Close() error {
	var errs []error
	if c.router != nil {
		for _, d := range c.router.AllDrivers() {
			if err := d.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if c.registry != nil {
		if err := c.registry.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (c *Client) resolveTarget(ctx context.Context, target string) (string, string, error) {
	return c.resolveTargetWithDeleted(ctx, target, false)
}

func (c *Client) resolveTargetWithDeleted(ctx context.Context, target string, allowDeleted bool) (string, string, error) {
	if target == "" {
		return "", "", ErrInvalidKey
	}

	// If registry is configured, check if target matches a logical ObjectID
	if c.registry != nil {
		rec, err := c.registry.GetByID(ctx, target)
		if err == nil && rec != nil {
			if !allowDeleted && rec.Status == StateDeleted {
				return "", "", WrapError("resolve_target", target, rec.Provider, ErrObjectNotFound)
			}
			return rec.Key, rec.Provider, nil
		}

		cleanKey := strings.TrimLeft(target, "/")
		recByKey, errByKey := c.registry.GetByKey(ctx, cleanKey)
		if errByKey == nil && recByKey != nil {
			if !allowDeleted && recByKey.Status == StateDeleted {
				return "", "", WrapError("resolve_target", cleanKey, recByKey.Provider, ErrObjectNotFound)
			}
			return recByKey.Key, recByKey.Provider, nil
		}

		if allowDeleted {
			recs, _ := c.registry.Find(ctx, Filter{ObjectID: target, Status: StateDeleted})
			if len(recs) > 0 {
				return recs[0].Key, recs[0].Provider, nil
			}
			recsByKey, _ := c.registry.Find(ctx, Filter{Key: cleanKey, Status: StateDeleted})
			if len(recsByKey) > 0 {
				return recsByKey[0].Key, recsByKey[0].Provider, nil
			}
		}
	}

	// If not found in registry or registry disabled, treat as physical key
	cleanKey := strings.TrimLeft(target, "/")
	return cleanKey, "", nil
}

func (c *Client) abortRegistry(ctx context.Context, objectID string) {
	if c.registry != nil && objectID != "" {
		_ = c.registry.UpdateStatus(ctx, objectID, StateAborted)
	}
}

// SoftDelete marks an object as deleted in the registry, hiding it from standard Get/Head
// queries without immediately destroying the underlying physical storage blob.
func (c *Client) SoftDelete(ctx context.Context, target string) error {
	if c.registry == nil {
		return fmt.Errorf("%w: soft delete requires metadata registry", ErrNotSupported)
	}

	start := time.Now()
	ctx = c.observer.OnOperationStart(ctx, OpSoftDelete, target)
	var err error
	defer func() {
		c.observer.OnOperationEnd(ctx, OpSoftDelete, target, time.Since(start), err)
	}()

	rec, _ := c.registry.GetByID(ctx, target)
	if rec == nil {
		rec, _ = c.registry.GetByKey(ctx, strings.TrimLeft(target, "/"))
	}
	if rec == nil {
		err = WrapError("soft_delete", target, "", ErrObjectNotFound)
		return err
	}

	if rec.LegalHold {
		err = WrapError("soft_delete", target, rec.Provider, ErrObjectLocked)
		return err
	}
	if rec.RetentionUntil != nil && rec.RetentionUntil.After(time.Now().UTC()) {
		err = WrapError("soft_delete", target, rec.Provider, ErrObjectLocked)
		return err
	}

	if err = c.registry.Delete(ctx, rec.ObjectID); err != nil {
		return WrapError("soft_delete", rec.ObjectID, rec.Provider, err)
	}

	if c.cache != nil {
		c.cache.Delete(rec.ObjectID)
		c.cache.Delete(rec.Key)
	}
	return nil
}

// Restore recovers a soft-deleted object, returning its lifecycle state to StateCommitted.
func (c *Client) Restore(ctx context.Context, target string) error {
	if c.registry == nil {
		return fmt.Errorf("%w: restore requires metadata registry", ErrNotSupported)
	}

	start := time.Now()
	ctx = c.observer.OnOperationStart(ctx, OpRestore, target)
	var err error
	defer func() {
		c.observer.OnOperationEnd(ctx, OpRestore, target, time.Since(start), err)
	}()

	rec, _ := c.registry.GetByID(ctx, target)
	if rec == nil {
		rec, _ = c.registry.GetByKey(ctx, strings.TrimLeft(target, "/"))
	}
	if rec == nil {
		recs, _ := c.registry.Find(ctx, Filter{ObjectID: target, Status: StateDeleted})
		if len(recs) > 0 {
			rec = &recs[0]
		} else {
			recsByKey, _ := c.registry.Find(ctx, Filter{Key: strings.TrimLeft(target, "/"), Status: StateDeleted})
			if len(recsByKey) > 0 {
				rec = &recsByKey[0]
			}
		}
	}
	if rec == nil {
		err = WrapError("restore", target, "", ErrObjectNotFound)
		return err
	}

	if rec.Status != StateDeleted {
		return nil
	}

	if err = c.registry.UpdateStatus(ctx, rec.ObjectID, StateCommitted); err != nil {
		return WrapError("restore", rec.ObjectID, rec.Provider, err)
	}

	if c.cache != nil {
		c.cache.Delete(rec.ObjectID)
		c.cache.Delete(rec.Key)
	}
	return nil
}

// PermanentDelete physically destroys an object from the storage backend and purges its metadata.
func (c *Client) PermanentDelete(ctx context.Context, target string) error {
	start := time.Now()
	ctx = c.observer.OnOperationStart(ctx, OpPermanentDelete, target)
	var err error
	defer func() {
		c.observer.OnOperationEnd(ctx, OpPermanentDelete, target, time.Since(start), err)
	}()

	key, providerName, resErr := c.resolveTargetWithDeleted(ctx, target, true)
	if resErr != nil {
		err = resErr
		return err
	}

	if c.registry != nil {
		rec, _ := c.registry.GetByID(ctx, target)
		if rec == nil {
			rec, _ = c.registry.GetByKey(ctx, key)
		}
		if rec == nil {
			recs, _ := c.registry.Find(ctx, Filter{ObjectID: target, Status: StateDeleted})
			if len(recs) > 0 {
				rec = &recs[0]
			} else {
				recsByKey, _ := c.registry.Find(ctx, Filter{Key: key, Status: StateDeleted})
				if len(recsByKey) > 0 {
					rec = &recsByKey[0]
				}
			}
		}
		if rec != nil {
			if rec.LegalHold {
				err = WrapError("permanent_delete", key, providerName, ErrObjectLocked)
				return err
			}
			if rec.RetentionUntil != nil && rec.RetentionUntil.After(time.Now().UTC()) {
				err = WrapError("permanent_delete", key, providerName, ErrObjectLocked)
				return err
			}
			_ = c.registry.HardDelete(ctx, rec.ObjectID)
		}
	}

	driver, rErr := c.router.Select(ctx, RouteContext{
		Op:             OpDelete,
		Key:            key,
		ForcedProvider: providerName,
	})
	if rErr != nil {
		err = WrapError("route_driver", key, "", rErr)
		return err
	}

	if delErr := driver.Delete(ctx, key); delErr != nil {
		c.router.ReportFailure(driver.Name(), delErr)
		err = delErr
		return err
	}
	c.router.ReportSuccess(driver.Name())

	if c.cache != nil {
		c.cache.Delete(target)
		c.cache.Delete(key)
	}
	return nil
}

// InitiateResumableUpload initializes a multipart upload session for large files or unstable networks.
// It assigns an ObjectID, physical storage key, creates a multipart session on the storage driver,
// and returns an UploadSession handle.
func (c *Client) InitiateResumableUpload(ctx context.Context, opts PutOptions, partSize int64, ttl time.Duration) (*UploadSession, error) {
	start := time.Now()
	ctx = c.observer.OnOperationStart(ctx, OpMultipart, opts.Filename)
	var err error
	defer func() {
		c.observer.OnOperationEnd(ctx, OpMultipart, opts.Filename, time.Since(start), err)
	}()

	// 1. Assign or generate canonical logical ObjectID
	objID := opts.ID
	if objID == "" {
		objID, err = key.NewObjectID()
		if err != nil {
			return nil, WrapError("initiate_resumable_upload", "", "", err)
		}
	}

	// 2. Sanitize and validate policy
	opts.Filename = SanitizeFilename(opts.Filename)
	opts.ContentDisposition = SanitizeHeader(opts.ContentDisposition)
	opts.CacheControl = SanitizeHeader(opts.CacheControl)

	activePolicy := opts.Policy
	if activePolicy == nil {
		activePolicy = c.policy
	}
	if activePolicy != nil {
		if err = activePolicy.Validate(ctx, ValidationInput{
			Namespace:   opts.Namespace,
			Filename:    opts.Filename,
			ContentType: opts.ContentType,
			Size:        opts.Size,
		}); err != nil {
			return nil, WrapError("policy_validation", "", "", err)
		}
	}

	// 3. Generate storage key
	storageKey := opts.Key
	if storageKey == "" {
		storageKey, err = c.keyGen.Generate(ctx, key.KeyInput{
			ID:        objID,
			Namespace: opts.Namespace,
			Filename:  opts.Filename,
			Ext:       key.NormalizeExtension(opts.Filename, ""),
			CreatedAt: time.Now().UTC(),
		})
		if err != nil {
			return nil, WrapError("generate_key", "", "", err)
		}
	}

	vis := opts.Visibility
	if vis == "" {
		vis = c.defaultVisibility
	}

	driver, rErr := c.router.Select(ctx, RouteContext{
		Op:             OpPut,
		Key:            storageKey,
		Namespace:      opts.Namespace,
		ForcedProvider: opts.Provider,
	})
	if rErr != nil {
		err = WrapError("route_driver", storageKey, "", rErr)
		return nil, err
	}

	if driver.Capabilities()&CapMultipartSession == 0 {
		err = fmt.Errorf("%w: driver %q does not support multipart uploads", ErrNotSupported, driver.Name())
		return nil, err
	}

	obj := &Object{
		ID:               objID,
		Namespace:        opts.Namespace,
		OwnerID:          opts.OwnerID,
		Key:              storageKey,
		ContentType:      opts.ContentType,
		OriginalFilename: opts.Filename,
		Visibility:       vis,
		Status:           StatePending,
		Metadata:         opts.Metadata,
		RetentionUntil:   opts.RetentionUntil,
		ExpiresAt:        opts.ExpiresAt,
		LegalHold:        opts.LegalHold,
		ClientChecksum:   opts.ClientChecksum,
		Provider:         driver.Name(),
		CreatedAt:        time.Now().UTC(),
	}

	uploadID, createErr := driver.CreateMultipart(ctx, obj, opts)
	if createErr != nil {
		c.router.ReportFailure(driver.Name(), createErr)
		err = createErr
		return nil, err
	}
	c.router.ReportSuccess(driver.Name())

	if partSize <= 0 {
		partSize = 5 * 1024 * 1024 // 5 MB minimum standard part size
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}

	sessionID, sErr := key.NewObjectID()
	if sErr != nil {
		_ = driver.AbortMultipart(ctx, storageKey, uploadID)
		err = sErr
		return nil, err
	}

	session := &UploadSession{
		ID:        sessionID,
		ObjectID:  objID,
		Key:       storageKey,
		Provider:  driver.Name(),
		UploadID:  uploadID,
		PartSize:  partSize,
		TotalSize: opts.Size,
		Parts:     make([]CompletedPart, 0),
		ExpiresAt: time.Now().UTC().Add(ttl),
		CreatedAt: time.Now().UTC(),
		Status:    SessionActive,
	}

	if c.registry != nil {
		rec := &Record{
			ObjectID:         obj.ID,
			Namespace:        obj.Namespace,
			OwnerID:          obj.OwnerID,
			Key:              obj.Key,
			Provider:         obj.Provider,
			MIMEType:         obj.ContentType,
			OriginalFilename: obj.OriginalFilename,
			Visibility:       obj.Visibility,
			Status:           StatePending,
			Metadata:         obj.Metadata,
			RetentionUntil:   obj.RetentionUntil,
			ExpiresAt:        obj.ExpiresAt,
			LegalHold:        obj.LegalHold,
			ClientChecksum:   obj.ClientChecksum,
			CreatedAt:        obj.CreatedAt,
		}
		if err = c.registry.Save(ctx, rec); err != nil {
			_ = driver.AbortMultipart(ctx, storageKey, uploadID)
			return nil, WrapError("registry_save_pending", storageKey, "", err)
		}
	}

	if err = c.saveSession(ctx, session); err != nil {
		_ = driver.AbortMultipart(ctx, storageKey, uploadID)
		return nil, WrapError("save_session", sessionID, "", err)
	}

	return session, nil
}

// UploadPart uploads a single chunk within an active resumable upload session.
func (c *Client) UploadPart(ctx context.Context, sessionID string, partNumber int, r io.Reader, size int64) (*CompletedPart, error) {
	if r == nil {
		return nil, ErrNilReader
	}
	if partNumber < 1 || partNumber > 10000 {
		return nil, fmt.Errorf("part number %d out of range (1-10000)", partNumber)
	}

	start := time.Now()
	ctx = c.observer.OnOperationStart(ctx, OpUploadPart, fmt.Sprintf("%s:%d", sessionID, partNumber))
	var err error
	defer func() {
		c.observer.OnOperationEnd(ctx, OpUploadPart, fmt.Sprintf("%s:%d", sessionID, partNumber), time.Since(start), err)
	}()

	session, gErr := c.getSession(ctx, sessionID)
	if gErr != nil {
		err = gErr
		return nil, err
	}

	if session.Status != SessionActive {
		err = fmt.Errorf("%w: session is %s", ErrSessionExpired, session.Status)
		return nil, err
	}
	if !session.ExpiresAt.IsZero() && time.Now().UTC().After(session.ExpiresAt) {
		err = ErrSessionExpired
		return nil, err
	}

	driver, rErr := c.router.Select(ctx, RouteContext{
		Op:             OpPut,
		Key:            session.Key,
		ForcedProvider: session.Provider,
	})
	if rErr != nil {
		err = WrapError("route_driver", session.Key, "", rErr)
		return nil, err
	}

	etag, uErr := driver.UploadPart(ctx, session.Key, session.UploadID, int32(partNumber), r, size)
	if uErr != nil {
		c.router.ReportFailure(driver.Name(), uErr)
		err = uErr
		return nil, err
	}
	c.router.ReportSuccess(driver.Name())
	c.observer.OnBytesTransferred(OpUploadPart, size)

	completed := CompletedPart{
		PartNumber: int32(partNumber),
		ETag:       etag,
		Size:       size,
	}

	// Update session parts
	found := false
	for i, p := range session.Parts {
		if p.PartNumber == int32(partNumber) {
			session.Parts[i] = completed
			found = true
			break
		}
	}
	if !found {
		session.Parts = append(session.Parts, completed)
	}

	if err = c.saveSession(ctx, session); err != nil {
		return nil, WrapError("save_session_part", sessionID, "", err)
	}

	return &completed, nil
}

// ListSessionParts retrieves all completed parts for the given upload session.
func (c *Client) ListSessionParts(ctx context.Context, sessionID string) ([]CompletedPart, error) {
	session, err := c.getSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	parts := make([]CompletedPart, len(session.Parts))
	copy(parts, session.Parts)
	return parts, nil
}

// CommitResumableUpload finalizes an active resumable upload session, assembling all parts
// into a completed object in storage and transitioning its registry record to StateCommitted.
func (c *Client) CommitResumableUpload(ctx context.Context, sessionID string) (*Object, error) {
	start := time.Now()
	ctx = c.observer.OnOperationStart(ctx, OpMultipart, sessionID)
	var err error
	defer func() {
		c.observer.OnOperationEnd(ctx, OpMultipart, sessionID, time.Since(start), err)
	}()

	session, gErr := c.getSession(ctx, sessionID)
	if gErr != nil {
		err = gErr
		return nil, err
	}

	if session.Status != SessionActive {
		err = fmt.Errorf("%w: session status is %s", ErrSessionExpired, session.Status)
		return nil, err
	}
	if !session.ExpiresAt.IsZero() && time.Now().UTC().After(session.ExpiresAt) {
		err = ErrSessionExpired
		return nil, err
	}
	if len(session.Parts) == 0 {
		err = fmt.Errorf("cannot commit session %s with zero parts", sessionID)
		return nil, err
	}

	driver, rErr := c.router.Select(ctx, RouteContext{
		Op:             OpPut,
		Key:            session.Key,
		ForcedProvider: session.Provider,
	})
	if rErr != nil {
		err = WrapError("route_driver", session.Key, "", rErr)
		return nil, err
	}

	obj := &Object{
		ID:       session.ObjectID,
		Key:      session.Key,
		Provider: session.Provider,
		Status:   StateCommitted,
	}

	if c.registry != nil {
		rec, _ := c.registry.GetByID(ctx, session.ObjectID)
		if rec != nil {
			obj.Namespace = rec.Namespace
			obj.OwnerID = rec.OwnerID
			obj.ContentType = rec.MIMEType
			obj.OriginalFilename = rec.OriginalFilename
			obj.Visibility = rec.Visibility
			obj.Metadata = rec.Metadata
			obj.RetentionUntil = rec.RetentionUntil
			obj.ExpiresAt = rec.ExpiresAt
			obj.LegalHold = rec.LegalHold
		}
	}

	completedObj, cErr := driver.CompleteMultipart(ctx, obj, session.UploadID, session.Parts)
	if cErr != nil {
		c.router.ReportFailure(driver.Name(), cErr)
		err = cErr
		return nil, err
	}
	c.router.ReportSuccess(driver.Name())

	session.Status = SessionCommitted
	_ = c.saveSession(ctx, session)

	if c.registry != nil {
		rec, _ := c.registry.GetByID(ctx, session.ObjectID)
		if rec != nil {
			rec.Bucket = completedObj.Bucket
			rec.Size = completedObj.Size
			rec.ETag = completedObj.ETag
			rec.ChecksumSHA256 = completedObj.ChecksumSHA256
			rec.Status = StateCommitted
			rec.UpdatedAt = time.Now().UTC()
			_ = c.registry.Save(ctx, rec)
		}
	}

	if c.cache != nil {
		c.cache.Delete(completedObj.ID)
		c.cache.Delete(completedObj.Key)
	}

	return completedObj, nil
}

// AbortResumableUpload aborts an in-progress multipart session and purges partial chunks.
func (c *Client) AbortResumableUpload(ctx context.Context, sessionID string) error {
	start := time.Now()
	ctx = c.observer.OnOperationStart(ctx, OpMultipart, sessionID)
	var err error
	defer func() {
		c.observer.OnOperationEnd(ctx, OpMultipart, sessionID, time.Since(start), err)
	}()

	session, gErr := c.getSession(ctx, sessionID)
	if gErr != nil {
		err = gErr
		return err
	}

	driver, rErr := c.router.Select(ctx, RouteContext{
		Op:             OpDelete,
		Key:            session.Key,
		ForcedProvider: session.Provider,
	})
	if rErr == nil && driver != nil {
		_ = driver.AbortMultipart(ctx, session.Key, session.UploadID)
	}

	session.Status = SessionAborted
	_ = c.saveSession(ctx, session)
	_ = c.deleteSession(ctx, sessionID)

	if c.registry != nil {
		c.abortRegistry(ctx, session.ObjectID)
	}

	return nil
}

// FindStaleSessions returns active sessions that have passed their expiration timestamp.
func (c *Client) FindStaleSessions(ctx context.Context, before time.Time, limit int) ([]UploadSession, error) {
	return c.findStaleSessions(ctx, before, limit)
}

// FindExpiredObjects returns records whose scheduled expiration has elapsed.
func (c *Client) FindExpiredObjects(ctx context.Context, before time.Time, limit int) ([]Record, error) {
	if c.registry == nil {
		return nil, nil
	}
	return c.registry.FindExpired(ctx, before, limit)
}

// FindSoftDeletedObjects returns records marked as soft-deleted before the specified cutoff.
func (c *Client) FindSoftDeletedObjects(ctx context.Context, before time.Time, limit int) ([]Record, error) {
	if c.registry == nil {
		return nil, nil
	}
	records, err := c.registry.Find(ctx, Filter{
		Status: StateDeleted,
		Limit:  limit,
	})
	if err != nil {
		return nil, err
	}
	var res []Record
	for _, r := range records {
		if r.DeletedAt != nil && !r.DeletedAt.After(before) {
			res = append(res, r)
		}
	}
	return res, nil
}

func (c *Client) saveSession(ctx context.Context, session *UploadSession) error {
	if c.registry != nil {
		return c.registry.SaveSession(ctx, session)
	}
	c.sessionsMu.Lock()
	defer c.sessionsMu.Unlock()
	cp := *session
	cp.Parts = make([]CompletedPart, len(session.Parts))
	copy(cp.Parts, session.Parts)
	c.sessions[session.ID] = &cp
	return nil
}

func (c *Client) getSession(ctx context.Context, sessionID string) (*UploadSession, error) {
	if c.registry != nil {
		return c.registry.GetSession(ctx, sessionID)
	}
	c.sessionsMu.RLock()
	defer c.sessionsMu.RUnlock()
	sess, ok := c.sessions[sessionID]
	if !ok || sess.Status == SessionAborted {
		return nil, ErrSessionNotFound
	}
	cp := *sess
	cp.Parts = make([]CompletedPart, len(sess.Parts))
	copy(cp.Parts, sess.Parts)
	return &cp, nil
}

func (c *Client) deleteSession(ctx context.Context, sessionID string) error {
	if c.registry != nil {
		return c.registry.DeleteSession(ctx, sessionID)
	}
	c.sessionsMu.Lock()
	defer c.sessionsMu.Unlock()
	delete(c.sessions, sessionID)
	return nil
}

func (c *Client) findStaleSessions(ctx context.Context, before time.Time, limit int) ([]UploadSession, error) {
	if c.registry != nil {
		return c.registry.FindStaleSessions(ctx, before, limit)
	}
	c.sessionsMu.RLock()
	defer c.sessionsMu.RUnlock()
	var stale []UploadSession
	for _, s := range c.sessions {
		if s.Status == SessionActive && !s.ExpiresAt.IsZero() && !s.ExpiresAt.After(before) {
			cp := *s
			stale = append(stale, cp)
			if limit > 0 && len(stale) >= limit {
				break
			}
		}
	}
	return stale, nil
}
