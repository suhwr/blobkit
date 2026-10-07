package blobkit

import (
	"bytes"
	"errors"
	"io"
	"sync"
)

// SizeUnknown represents an unknown or unbounded streaming payload size.
const SizeUnknown int64 = -1

// SizeReader wraps an io.Reader to enforce expected payload size constraints.
// If exact is true (or expectedSize >= 0), it returns ErrSizeMismatch if the stream
// yields fewer bytes than expected or has surplus bytes beyond expectedSize.
type SizeReader struct {
	mu           sync.Mutex
	r            io.Reader
	expectedSize int64
	exact        bool
	readCount    int64
	eofChecked   bool
	mismatchErr  error
}

// NewSizeReader constructs a SizeReader.
// If exact is true and expectedSize >= 0, the reader strictly verifies byte parity.
// If expectedSize < 0, size verification is disabled (pass-through).
func NewSizeReader(r io.Reader, expectedSize int64, exact bool) *SizeReader {
	if expectedSize < 0 {
		exact = false
	}
	if existing, ok := r.(*SizeReader); ok {
		if existing.expectedSize == expectedSize && existing.exact == exact {
			return existing
		}
	}
	return &SizeReader{
		r:            r,
		expectedSize: expectedSize,
		exact:        exact,
	}
}

// Read implements io.Reader with strict boundary checking.
func (sr *SizeReader) Read(p []byte) (n int, err error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.mismatchErr != nil {
		return 0, sr.mismatchErr
	}

	if !sr.exact {
		return sr.r.Read(p)
	}

	remaining := sr.expectedSize - sr.readCount
	if remaining > 0 {
		toRead := p
		if int64(len(toRead)) > remaining {
			toRead = toRead[:remaining]
		}
		n, err = sr.r.Read(toRead)
		sr.readCount += int64(n)
		if err == io.EOF {
			if sr.readCount < sr.expectedSize {
				sr.mismatchErr = ErrSizeMismatch
				return n, ErrSizeMismatch
			}
			sr.eofChecked = true
			if n > 0 {
				return n, nil
			}
			return 0, io.EOF
		}
		if err != nil {
			return n, err
		}
		return n, nil
	}

	// At or beyond expectedSize: verify no extra bytes exist in the underlying stream
	if !sr.eofChecked {
		var extra [1]byte
		nExtra, extraErr := sr.r.Read(extra[:])
		sr.eofChecked = true
		if nExtra > 0 {
			sr.mismatchErr = ErrSizeMismatch
			return 0, ErrSizeMismatch
		}
		if errors.Is(extraErr, ErrSizeMismatch) {
			sr.mismatchErr = ErrSizeMismatch
			return 0, ErrSizeMismatch
		}
		if extraErr != nil && extraErr != io.EOF {
			sr.mismatchErr = extraErr
			return 0, extraErr
		}
	}

	if sr.mismatchErr != nil {
		return 0, sr.mismatchErr
	}

	return 0, io.EOF
}

// TotalRead returns the total number of bytes read so far.
func (sr *SizeReader) TotalRead() int64 {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return sr.readCount
}

// Verify checks whether the reader was read to completion and matched expectedSize.
func (sr *SizeReader) Verify() error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if !sr.exact {
		return nil
	}
	if sr.mismatchErr != nil {
		return sr.mismatchErr
	}
	if sr.readCount != sr.expectedSize {
		sr.mismatchErr = ErrSizeMismatch
		return ErrSizeMismatch
	}
	if !sr.eofChecked {
		var extra [1]byte
		nExtra, err := sr.r.Read(extra[:])
		sr.eofChecked = true
		if nExtra > 0 {
			sr.mismatchErr = ErrSizeMismatch
			return ErrSizeMismatch
		}
		if errors.Is(err, ErrSizeMismatch) {
			sr.mismatchErr = ErrSizeMismatch
			return ErrSizeMismatch
		}
		if err != nil && err != io.EOF {
			sr.mismatchErr = err
			return err
		}
	}
	return sr.mismatchErr
}

// ResolvePayload inspects the reader and PutOptions to determine exact size semantics.
// It handles the distinction between known 0-byte objects and unassigned stream sizes.
func ResolvePayload(r io.Reader, opts PutOptions) (io.Reader, int64, bool, error) {
	if r == nil {
		return nil, 0, false, ErrNilReader
	}

	// Case 1: Caller explicitly set opts.ExplicitSize or opts.Size > 0
	if opts.ExplicitSize || opts.Size > 0 {
		sr := NewSizeReader(r, opts.Size, true)
		return sr, opts.Size, true, nil
	}

	// Case 2: Caller set opts.Size < 0 (unambiguous streaming/unknown size)
	if opts.Size < 0 {
		return r, SizeUnknown, false, nil
	}

	// Case 3: opts.Size == 0 without ExplicitSize
	// Check if reader exposes known length (e.g. bytes.Reader, strings.Reader, bytes.Buffer)
	if lr, ok := r.(interface{ Len() int }); ok {
		l := int64(lr.Len())
		if l == 0 {
			// Explicit empty reader
			return NewSizeReader(r, 0, true), 0, true, nil
		}
		// Reader has data, so caller defaulted PutOptions{}. Size is l.
		return NewSizeReader(r, l, true), l, true, nil
	}

	// For general streams where size is 0 and !ExplicitSize, peek 1 byte to differentiate empty vs stream
	var peek [1]byte
	var n int
	var err error
	for i := 0; i < 100; i++ {
		n, err = r.Read(peek[:])
		if n > 0 || err != nil {
			break
		}
	}
	if err == io.EOF && n == 0 {
		return bytes.NewReader(nil), 0, true, nil
	}
	if err != nil {
		return nil, 0, false, err
	}

	// Non-empty stream with unassigned size: prepend peeked byte and mark as SizeUnknown
	chained := io.MultiReader(bytes.NewReader(peek[:n]), r)
	return chained, SizeUnknown, false, nil
}
