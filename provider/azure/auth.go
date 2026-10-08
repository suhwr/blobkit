package azure

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// signRequest signs the outgoing HTTP request using Azure SharedKey authorization.
func signRequest(req *http.Request, accountName, accountKey, apiVersion string) error {
	if accountKey == "" {
		return nil
	}

	keyBytes, err := base64.StdEncoding.DecodeString(accountKey)
	if err != nil {
		return fmt.Errorf("blobkit/azure: invalid base64 AccountKey: %w", err)
	}

	// Set required headers
	nowStr := time.Now().UTC().Format(http.TimeFormat)
	req.Header.Set("x-ms-date", nowStr)
	req.Header.Set("x-ms-version", apiVersion)

	stringToSign := buildStringToSign(req, accountName)

	h := hmac.New(sha256.New, keyBytes)
	h.Write([]byte(stringToSign))
	sig := base64.StdEncoding.EncodeToString(h.Sum(nil))

	req.Header.Set("Authorization", fmt.Sprintf("SharedKey %s:%s", accountName, sig))
	return nil
}

// buildStringToSign constructs the canonical string representation for Azure Blob Storage REST API.
func buildStringToSign(req *http.Request, accountName string) string {
	contentLengthStr := ""
	if req.ContentLength > 0 {
		contentLengthStr = strconv.FormatInt(req.ContentLength, 10)
	} else if req.ContentLength == 0 && (req.Method == http.MethodPut || req.Method == http.MethodPost) {
		contentLengthStr = "0"
	}

	canonicalizedHeaders := buildCanonicalizedHeaders(req.Header)
	canonicalizedResource := buildCanonicalizedResource(req.URL, accountName)

	parts := []string{
		req.Method,
		req.Header.Get("Content-Encoding"),
		req.Header.Get("Content-Language"),
		contentLengthStr,
		req.Header.Get("Content-MD5"),
		req.Header.Get("Content-Type"),
		"", // Date is omitted when x-ms-date is provided
		req.Header.Get("If-Modified-Since"),
		req.Header.Get("If-Match"),
		req.Header.Get("If-None-Match"),
		req.Header.Get("If-Unmodified-Since"),
		req.Header.Get("Range"),
		canonicalizedHeaders + canonicalizedResource,
	}

	return strings.Join(parts, "\n")
}

// buildCanonicalizedHeaders sorts and formats all x-ms-* headers.
func buildCanonicalizedHeaders(headers http.Header) string {
	var msHeaders []string
	headerMap := make(map[string]string)

	for k, v := range headers {
		lowerK := strings.ToLower(k)
		if strings.HasPrefix(lowerK, "x-ms-") {
			trimmedVal := strings.TrimSpace(strings.Join(v, ","))
			if _, exists := headerMap[lowerK]; !exists {
				msHeaders = append(msHeaders, lowerK)
			}
			headerMap[lowerK] = trimmedVal
		}
	}

	sort.Strings(msHeaders)

	var sb strings.Builder
	for _, h := range msHeaders {
		sb.WriteString(h)
		sb.WriteString(":")
		sb.WriteString(headerMap[h])
		sb.WriteString("\n")
	}

	return sb.String()
}

// buildCanonicalizedResource formats the path and sorted query parameters.
func buildCanonicalizedResource(u *url.URL, accountName string) string {
	var sb strings.Builder
	sb.WriteString("/")
	sb.WriteString(accountName)
	path := u.EscapedPath()
	if path == "" {
		path = u.Path
	}
	sb.WriteString(path)

	query := u.Query()
	if len(query) > 0 {
		lowerQuery := make(map[string][]string, len(query))
		var paramNames []string
		for k, v := range query {
			lk := strings.ToLower(k)
			if _, exists := lowerQuery[lk]; !exists {
				paramNames = append(paramNames, lk)
			}
			lowerQuery[lk] = append(lowerQuery[lk], v...)
		}
		sort.Strings(paramNames)

		for _, p := range paramNames {
			vals := lowerQuery[p]
			sort.Strings(vals)
			sb.WriteString("\n")
			sb.WriteString(p)
			sb.WriteString(":")
			sb.WriteString(strings.Join(vals, ","))
		}
	}

	return sb.String()
}

// generateBlobSAS creates a signed Shared Access Signature token for direct blob read/write.
func generateBlobSAS(accountName, accountKey, container, blob, permissions, apiVersion string, expiry time.Time) (string, error) {
	keyBytes, err := base64.StdEncoding.DecodeString(accountKey)
	if err != nil {
		return "", fmt.Errorf("blobkit/azure: invalid base64 AccountKey: %w", err)
	}

	cleanBlob := strings.Trim(blob, "/")
	canonicalizedResource := fmt.Sprintf("/blob/%s/%s/%s", accountName, container, cleanBlob)
	expiryStr := expiry.UTC().Format("2006-01-02T15:04:05Z")

	// Azure Blob Service SAS StringToSign format (version 2020-10-02+)
	parts := []string{
		permissions,           // signedPermissions (e.g. "r", "w")
		"",                    // signedStart
		expiryStr,             // signedExpiry
		canonicalizedResource, // canonicalizedResource
		"",                    // signedIdentifier
		"",                    // signedIP
		"https",               // signedProtocol
		apiVersion,            // signedVersion
		"b",                   // signedResource (b = blob)
		"",                    // signedSnapshotTime
		"",                    // signedEncryptionScope
		"",                    // rscc
		"",                    // rscd
		"",                    // rsce
		"",                    // rscl
		"",                    // rsct
	}

	stringToSign := strings.Join(parts, "\n")

	h := hmac.New(sha256.New, keyBytes)
	h.Write([]byte(stringToSign))
	sig := base64.StdEncoding.EncodeToString(h.Sum(nil))

	params := url.Values{}
	params.Set("sp", permissions)
	params.Set("se", expiryStr)
	params.Set("spr", "https")
	params.Set("sv", apiVersion)
	params.Set("sr", "b")
	params.Set("sig", sig)

	return params.Encode(), nil
}
