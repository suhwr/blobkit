package gcs

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/suhwr/blobkit"
)

// authorizeRequest injects the OAuth2 Bearer token into the HTTP request headers.
func (d *Driver) authorizeRequest(ctx context.Context, req *http.Request) error {
	var token string
	if d.cfg.TokenFunc != nil {
		t, err := d.cfg.TokenFunc(ctx)
		if err != nil {
			return blobkit.WrapError("auth", "", d.cfg.Name, fmt.Errorf("failed to retrieve token: %w", err))
		}
		token = t
	} else {
		token = d.cfg.BearerToken
	}

	if token == "" {
		return blobkit.WrapError("auth", "", d.cfg.Name, errors.New("empty bearer token"))
	}

	req.Header.Set("Authorization", "Bearer "+token)
	return nil
}

// buildV4SignedURL generates an official Google Cloud Storage V4 Signed URL.
func (d *Driver) buildV4SignedURL(ctx context.Context, method, key string, opts blobkit.PresignOptions) (*blobkit.PresignedURL, error) {
	if d.cfg.parsedPrivateKey == nil && d.cfg.SignBytesFunc == nil {
		return nil, blobkit.WrapError("presign", key, d.cfg.Name, blobkit.ErrUnsupportedOperation)
	}

	cleanKey := strings.TrimLeft(key, "/")
	if cleanKey == "" {
		return nil, blobkit.ErrInvalidKey
	}

	lifetime := opts.Expiry
	if lifetime <= 0 {
		lifetime = blobkit.DefaultPresignExpiry
	}
	// Max lifetime for GCS V4 signed URLs is 7 days (604800 seconds)
	if lifetime > 7*24*time.Hour {
		lifetime = 7 * 24 * time.Hour
	}

	now := time.Now().UTC()
	dateStr := now.Format("20060102")
	timestampStr := now.Format("20060102T150405Z")
	expiresAt := now.Add(lifetime)
	expiresSec := int(lifetime.Seconds())

	// Resolve host and canonical URI
	host := "storage.googleapis.com"
	if d.cfg.PublicBaseURL != "" {
		if u, err := url.Parse(d.cfg.PublicBaseURL); err == nil && u.Host != "" {
			host = u.Host
		}
	}

	// Canonical URI: /{bucket}/{escapedKey}
	parts := strings.Split(cleanKey, "/")
	escapedParts := make([]string, len(parts))
	for i, p := range parts {
		escapedParts[i] = url.PathEscape(p)
	}
	escapedKeyPath := strings.Join(escapedParts, "/")
	canonicalURI := fmt.Sprintf("/%s/%s", url.PathEscape(d.cfg.Bucket), escapedKeyPath)

	// Prepare signed headers
	signedHeadersMap := make(map[string]string)
	signedHeadersMap["host"] = host

	if opts.ContentType != "" && method == http.MethodPut {
		signedHeadersMap["content-type"] = opts.ContentType
	}
	if opts.ContentDisposition != "" {
		signedHeadersMap["content-disposition"] = opts.ContentDisposition
	}

	// Canonical Query parameters
	query := make(url.Values)
	if opts.QueryParams != nil {
		for k, v := range opts.QueryParams {
			for _, val := range v {
				query.Add(k, val)
			}
		}
	}

	credentialScope := fmt.Sprintf("%s/%s/auto/storage/goog4_request", d.cfg.ServiceAccountEmail, dateStr)
	query.Set("X-Goog-Algorithm", "GOOG4-RSA-SHA256")
	query.Set("X-Goog-Credential", credentialScope)
	query.Set("X-Goog-Date", timestampStr)
	query.Set("X-Goog-Expires", fmt.Sprintf("%d", expiresSec))

	// Sort and format signed header names
	var headerNames []string
	for k := range signedHeadersMap {
		headerNames = append(headerNames, strings.ToLower(k))
	}
	sort.Strings(headerNames)
	signedHeadersList := strings.Join(headerNames, ";")
	query.Set("X-Goog-SignedHeaders", signedHeadersList)

	// Build Canonical Query String
	canonicalQuery := query.Encode()

	// Build Canonical Headers string
	var canonicalHeadersBuilder strings.Builder
	for _, h := range headerNames {
		val := strings.TrimSpace(signedHeadersMap[h])
		canonicalHeadersBuilder.WriteString(fmt.Sprintf("%s:%s\n", h, val))
	}
	canonicalHeaders := canonicalHeadersBuilder.String()

	// Canonical Request
	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\nUNSIGNED-PAYLOAD",
		method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeadersList,
	)

	// String To Sign
	canonicalReqHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := fmt.Sprintf("GOOG4-RSA-SHA256\n%s\n%s/%s/auto/storage/goog4_request\n%s",
		timestampStr,
		d.cfg.ServiceAccountEmail,
		dateStr,
		hex.EncodeToString(canonicalReqHash[:]),
	)

	// Sign StringToSign using RSA-SHA256
	var signatureBytes []byte
	if d.cfg.SignBytesFunc != nil {
		sig, err := d.cfg.SignBytesFunc(ctx, []byte(stringToSign))
		if err != nil {
			return nil, blobkit.WrapError("presign", key, d.cfg.Name, fmt.Errorf("SignBytesFunc failed: %w", err))
		}
		signatureBytes = sig
	} else {
		hash := sha256.Sum256([]byte(stringToSign))
		sig, err := rsa.SignPKCS1v15(rand.Reader, d.cfg.parsedPrivateKey, crypto.SHA256, hash[:])
		if err != nil {
			return nil, blobkit.WrapError("presign", key, d.cfg.Name, fmt.Errorf("RSA signing failed: %w", err))
		}
		signatureBytes = sig
	}

	hexSignature := hex.EncodeToString(signatureBytes)

	// Build Final URL
	finalURL := fmt.Sprintf("https://%s%s?%s&X-Goog-Signature=%s", host, canonicalURI, canonicalQuery, hexSignature)

	return &blobkit.PresignedURL{
		URL:           finalURL,
		Method:        method,
		ExpiresAt:     expiresAt,
		SignedHeaders: signedHeadersMap,
	}, nil
}
