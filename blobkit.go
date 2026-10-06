package blobkit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
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
}

// New constructs a new BlobKit Client configured with the provided options.
func New(options ...Option) (*Client, error) {
	c := &Client{
		keyGen:            key.NewUUIDv7Generator(),
		defaultVisibility: VisibilityPrivate,
	}

	for _, opt := range options {
		opt(c)
	}

	if c.router == nil {
		return nil, fmt.Errorf("%w: at least one driver or router must be configured", ErrProviderUnavailable)
	}

	return c, nil
}

// Put uploads an object stream based on the caller's semantic intent.
// It assigns a canonical logical ObjectID, generates a physical storage key,
// peeks MIME types without buffering entire files, and commits metadata to the registry if enabled.
func (c *Client) Put(ctx context.Context, r io.Reader, opts PutOptions) (*Object, error) {
	if r == nil {
		return nil, ErrNilReader
	}

	// 1. Assign or generate canonical logical ObjectID (UUIDv7)
	objID := opts.ID
	if objID == "" {
		var err error
		objID, err = key.NewObjectID()
		if err != nil {
			return nil, WrapError("put", "", "", err)
		}
	}

	// 2. Peek MIME type without consuming the stream into memory
	reconstructedReader, detectedMIME, err := mime.Sniff(r, opts.Filename, opts.ContentType)
	if err != nil {
		return nil, WrapError("sniff_mime", "", "", err)
	}

	// 3. Formulate physical storage key
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
		CreatedAt:        time.Now().UTC(),
	}

	// 4. Record initial pending state in database registry if enabled
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
			CreatedAt:        obj.CreatedAt,
		}
		if saveErr := c.registry.Save(ctx, rec); saveErr != nil {
			return nil, WrapError("registry_save_pending", obj.Key, "", saveErr)
		}
	}

	// 5. Select driver from router
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

	// 6. Perform physical upload
	savedObj, uploadErr := driver.Put(ctx, obj, reconstructedReader, opts)
	if uploadErr != nil {
		c.router.ReportFailure(driver.Name(), uploadErr)
		c.abortRegistry(ctx, obj.ID)
		return nil, uploadErr
	}
	c.router.ReportSuccess(driver.Name())

	savedObj.Status = StateCommitted

	// 7. Commit metadata record in database registry
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
			CreatedAt:        savedObj.CreatedAt,
			UpdatedAt:        savedObj.UpdatedAt,
		}
		if err := c.registry.Save(ctx, rec); err != nil {
			return savedObj, WrapError("registry_save_committed", savedObj.Key, savedObj.Provider, err)
		}
	}

	return savedObj, nil
}

// Get retrieves an object stream. Target can be either a canonical logical ObjectID
// (when registry is enabled) or a physical storage key.
func (c *Client) Get(ctx context.Context, target string, opts GetOptions) (*ObjectReader, error) {
	key, providerName, err := c.resolveTarget(ctx, target)
	if err != nil {
		return nil, err
	}

	driver, err := c.router.Select(ctx, RouteContext{
		Op:             OpGet,
		Key:            key,
		ForcedProvider: providerName,
	})
	if err != nil {
		return nil, WrapError("route_driver", key, "", err)
	}

	reader, err := driver.Get(ctx, key, opts)
	if err != nil {
		c.router.ReportFailure(driver.Name(), err)
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
func (c *Client) Head(ctx context.Context, target string) (*Object, error) {
	key, providerName, err := c.resolveTarget(ctx, target)
	if err != nil {
		return nil, err
	}

	driver, err := c.router.Select(ctx, RouteContext{
		Op:             OpHead,
		Key:            key,
		ForcedProvider: providerName,
	})
	if err != nil {
		return nil, WrapError("route_driver", key, "", err)
	}

	obj, err := driver.Head(ctx, key)
	if err != nil {
		c.router.ReportFailure(driver.Name(), err)
		return nil, err
	}
	c.router.ReportSuccess(driver.Name())

	if target != key {
		obj.ID = target
	}
	return obj, nil
}

// Delete removes an object from storage and marks or deletes it in the registry.
func (c *Client) Delete(ctx context.Context, target string) error {
	key, providerName, err := c.resolveTarget(ctx, target)
	if err != nil {
		return err
	}

	driver, err := c.router.Select(ctx, RouteContext{
		Op:             OpDelete,
		Key:            key,
		ForcedProvider: providerName,
	})
	if err != nil {
		return WrapError("route_driver", key, "", err)
	}

	if delErr := driver.Delete(ctx, key); delErr != nil {
		c.router.ReportFailure(driver.Name(), delErr)
		return delErr
	}
	c.router.ReportSuccess(driver.Name())

	if c.registry != nil {
		_ = c.registry.UpdateStatus(ctx, target, StateDeleted)
	}

	return nil
}

// DeleteBatch removes multiple objects in a single batch operation.
func (c *Client) DeleteBatch(ctx context.Context, targets []string) ([]string, error) {
	if len(targets) == 0 {
		return nil, nil
	}

	driver, err := c.router.Select(ctx, RouteContext{Op: OpDelete})
	if err != nil {
		return nil, WrapError("route_driver", "", "", err)
	}

	// Resolve any ObjectIDs to physical keys
	resolvedKeys := make([]string, len(targets))
	for i, t := range targets {
		k, _, _ := c.resolveTarget(ctx, t)
		resolvedKeys[i] = k
	}

	deleted, err := driver.DeleteBatch(ctx, resolvedKeys)
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

	return deleted, nil
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
	if target == "" {
		return "", "", ErrInvalidKey
	}

	// If registry is configured, check if target matches a logical ObjectID
	if c.registry != nil {
		rec, err := c.registry.GetByID(ctx, target)
		if err == nil && rec != nil {
			return rec.Key, rec.Provider, nil
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
