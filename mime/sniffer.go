package mime

import (
	"bytes"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strings"
)

// SniffSize is the standard number of bytes required by http.DetectContentType.
const SniffSize = 512

// Sniff peeks up to 512 bytes from r to detect the MIME type, then reconstructs the reader
// using io.MultiReader so zero stream bytes are lost and the entire file is not buffered into memory.
// If hintMIME is provided and valid, it takes precedence.
// If filename has a recognizable extension, it is used when content sniffing returns generic octet-stream/text-plain.
func Sniff(r io.Reader, filename, hintMIME string) (io.Reader, string, error) {
	if r == nil {
		return nil, "", io.ErrUnexpectedEOF
	}

	trimmedHint := strings.TrimSpace(hintMIME)
	if trimmedHint != "" && trimmedHint != "application/octet-stream" {
		return r, trimmedHint, nil
	}

	// If r is an io.ReadSeeker, we can read the peek buffer and seek back to the starting offset.
	if seeker, ok := r.(io.ReadSeeker); ok {
		currentOffset, posErr := seeker.Seek(0, io.SeekCurrent)
		if posErr == nil {
			buf := make([]byte, SniffSize)
			n, err := seeker.Read(buf)
			if err != nil && err != io.EOF {
				_, _ = seeker.Seek(currentOffset, io.SeekStart)
				return r, "application/octet-stream", err
			}
			// Reset back to starting position
			if _, seekErr := seeker.Seek(currentOffset, io.SeekStart); seekErr != nil {
				return r, "application/octet-stream", seekErr
			}
			detected := detectMIME(buf[:n], filename)
			return seeker, detected, nil
		}
	}

	// General streaming io.Reader: read up to 512 bytes without exhausting the stream
	buf := make([]byte, SniffSize)
	n, err := io.ReadFull(r, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return r, "application/octet-stream", err
	}

	detected := detectMIME(buf[:n], filename)
	reconstructed := io.MultiReader(bytes.NewReader(buf[:n]), r)
	return reconstructed, detected, nil
}

// SniffCloser is like Sniff, but preserves io.ReadCloser semantics.
func SniffCloser(rc io.ReadCloser, filename, hintMIME string) (io.ReadCloser, string, error) {
	if rc == nil {
		return nil, "", io.ErrUnexpectedEOF
	}

	r, contentType, err := Sniff(rc, filename, hintMIME)
	if err != nil {
		return rc, contentType, err
	}

	return &readCloserWrapper{
		Reader: r,
		Closer: rc,
	}, contentType, nil
}

type readCloserWrapper struct {
	io.Reader
	io.Closer
}

// detectMIME inspects magic bytes and falls back to extension mapping if sniffing yields generic binary.
func detectMIME(sample []byte, filename string) string {
	if len(sample) == 0 {
		if ext := filepath.Ext(filename); ext != "" {
			if t := mime.TypeByExtension(ext); t != "" {
				return cleanMIME(t)
			}
		}
		return "application/octet-stream"
	}

	// Check custom magic signatures that standard http.DetectContentType misses or misclassifies
	if detected := checkKnownSignatures(sample); detected != "" {
		return detected
	}

	// Use standard Go http.DetectContentType
	detected := http.DetectContentType(sample)

	// If detected is generic octet-stream or text/plain, try file extension
	if detected == "application/octet-stream" || detected == "text/plain; charset=utf-8" {
		if ext := filepath.Ext(filename); ext != "" {
			if t := mime.TypeByExtension(ext); t != "" {
				return cleanMIME(t)
			}
			if custom := extensionFallback(strings.ToLower(ext)); custom != "" {
				return custom
			}
		}
	}

	return cleanMIME(detected)
}

func checkKnownSignatures(sample []byte) string {
	// WebP: RIFF ???? WEBP
	if len(sample) >= 12 &&
		string(sample[0:4]) == "RIFF" &&
		string(sample[8:12]) == "WEBP" {
		return "image/webp"
	}

	// Ogg Vorbis/Opus
	if len(sample) >= 4 && string(sample[0:4]) == "OggS" {
		return "audio/ogg"
	}

	// MP4: ????ftyp
	if len(sample) >= 8 && string(sample[4:8]) == "ftyp" {
		return "video/mp4"
	}

	// SVG XML
	trimmed := bytes.TrimSpace(sample)
	if bytes.HasPrefix(trimmed, []byte("<?xml")) || bytes.HasPrefix(trimmed, []byte("<svg")) {
		if bytes.Contains(trimmed, []byte("<svg")) {
			return "image/svg+xml"
		}
	}

	// JSON
	if (bytes.HasPrefix(trimmed, []byte("{")) && bytes.HasSuffix(trimmed, []byte("}"))) ||
		(bytes.HasPrefix(trimmed, []byte("[")) && bytes.HasSuffix(trimmed, []byte("]"))) {
		return "application/json"
	}

	return ""
}

func extensionFallback(ext string) string {
	switch ext {
	case ".webp":
		return "image/webp"
	case ".json":
		return "application/json"
	case ".svg":
		return "image/svg+xml"
	case ".mp4":
		return "video/mp4"
	case ".mp3":
		return "audio/mpeg"
	case ".ogg":
		return "audio/ogg"
	case ".wav":
		return "audio/wav"
	case ".pdf":
		return "application/pdf"
	case ".csv":
		return "text/csv"
	default:
		return ""
	}
}

func cleanMIME(m string) string {
	if idx := strings.Index(m, ";"); idx != -1 {
		return strings.TrimSpace(m[:idx])
	}
	return strings.TrimSpace(m)
}
