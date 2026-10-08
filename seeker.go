package blobkit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// SeekableReader implements io.ReadSeekCloser and io.ReaderAt over an object storage key
// using HTTP Byte-Range requests ("Range: bytes=start-end").
//
// It enables fast, on-demand random-access seeking (e.g. for ZIP parsing, Parquet query engines,
// audio/video streaming, or PDF reading) without downloading the entire object into memory or disk.
type SeekableReader struct {
	ctx        context.Context
	bucket     *Bucket
	key        string
	size       int64
	cursor     int64
	activeBody io.ReadCloser
	mu         sync.Mutex
	closed     bool
}

// OpenSeeker creates an io.ReadSeekCloser and io.ReaderAt over the given object key.
// It verifies the driver's CapByteRangeGet capability and fetches the object's total size via Head.
func (b *Bucket) OpenSeeker(ctx context.Context, key string) (*SeekableReader, error) {
	if err := ValidateKey(key); err != nil {
		return nil, WrapError("open_seeker", key, b.driver.Name(), err)
	}
	if b.driver.Capabilities()&CapByteRangeGet == 0 {
		return nil, WrapError("open_seeker", key, b.driver.Name(), ErrUnsupportedOperation)
	}

	head, err := b.Head(ctx, key)
	if err != nil {
		return nil, err
	}

	return &SeekableReader{
		ctx:    ctx,
		bucket: b,
		key:    key,
		size:   head.Size,
		cursor: 0,
	}, nil
}

// Size returns the total size of the remote object in bytes.
func (s *SeekableReader) Size() int64 {
	return s.size
}

// Seek sets the offset for the next Read, adhering to the standard io.Seeker contract.
func (s *SeekableReader) Seek(offset int64, whence int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, errors.New("blobkit: seeker is closed")
	}

	var newCursor int64
	switch whence {
	case io.SeekStart:
		newCursor = offset
	case io.SeekCurrent:
		newCursor = s.cursor + offset
	case io.SeekEnd:
		newCursor = s.size + offset
	default:
		return 0, errors.New("blobkit: invalid seek whence")
	}

	if newCursor < 0 {
		return 0, errors.New("blobkit: negative seek offset")
	}

	if newCursor != s.cursor && s.activeBody != nil {
		_ = s.activeBody.Close()
		s.activeBody = nil
	}

	s.cursor = newCursor
	return s.cursor, nil
}

// Read reads up to len(p) bytes from the remote object starting at the current cursor.
func (s *SeekableReader) Read(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, errors.New("blobkit: seeker is closed")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if s.cursor >= s.size {
		return 0, io.EOF
	}

	if s.activeBody == nil {
		rangeHeader := fmt.Sprintf("bytes=%d-%d", s.cursor, s.size-1)
		reader, err := s.bucket.Get(s.ctx, s.key, GetOptions{Range: rangeHeader})
		if err != nil {
			return 0, err
		}
		s.activeBody = reader.Body
	}

	n, err := s.activeBody.Read(p)
	s.cursor += int64(n)

	if err == io.EOF {
		_ = s.activeBody.Close()
		s.activeBody = nil
		if s.cursor < s.size {
			// Partial range completed, clear EOF so subsequent reads can continue to the end
			err = nil
		}
	}

	return n, err
}

// ReadAt reads len(p) bytes into p at offset off without modifying the seeker's cursor.
// It implements io.ReaderAt and is safe for concurrent execution across goroutines.
func (s *SeekableReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("blobkit: negative offset in ReadAt")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= s.size {
		return 0, io.EOF
	}

	end := off + int64(len(p)) - 1
	if end >= s.size {
		end = s.size - 1
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, end)
	reader, err := s.bucket.Get(s.ctx, s.key, GetOptions{Range: rangeHeader})
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	n, err := io.ReadFull(reader, p[:end-off+1])
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		if off+int64(n) >= s.size {
			err = io.EOF
		}
	}
	return n, err
}

// Close closes any open active stream connections.
func (s *SeekableReader) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	if s.activeBody != nil {
		err := s.activeBody.Close()
		s.activeBody = nil
		return err
	}
	return nil
}
