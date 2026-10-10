package gdrive

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestOAuthTokenSource_Direct(t *testing.T) {
	// 1. Missing fields validation
	_, err := NewOAuthTokenSource("", "secret", "refresh")
	if err == nil {
		t.Fatal("expected error for empty client_id")
	}
	_, err = NewOAuthTokenSource("id", "", "refresh")
	if err == nil {
		t.Fatal("expected error for empty client_secret")
	}
	_, err = NewOAuthTokenSource("id", "secret", "")
	if err == nil {
		t.Fatal("expected error for empty refresh_token")
	}

	// 2. Token refresh and caching flow
	var reqCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqCount, 1)

		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form err: %v", err)
		}

		if r.Form.Get("grant_type") != "refresh_token" {
			t.Errorf("expected grant_type refresh_token, got %s", r.Form.Get("grant_type"))
		}
		if r.Form.Get("client_id") != "test-client-id" {
			t.Errorf("expected client_id test-client-id, got %s", r.Form.Get("client_id"))
		}
		if r.Form.Get("client_secret") != "test-client-secret" {
			t.Errorf("expected client_secret test-client-secret, got %s", r.Form.Get("client_secret"))
		}
		if r.Form.Get("refresh_token") != "test-refresh-token" {
			t.Errorf("expected refresh_token test-refresh-token, got %s", r.Form.Get("refresh_token"))
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "mock-access-token-123",
			"expires_in":   3600,
			"token_type":   "Bearer",
		})
	}))
	defer server.Close()

	src, err := NewOAuthTokenSource("test-client-id", "test-client-secret", "test-refresh-token")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	src.SetTokenURI(server.URL)

	ctx := context.Background()

	// First call -> hits server
	token, err := src.Token(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "mock-access-token-123" {
		t.Errorf("expected token mock-access-token-123, got %s", token)
	}
	if atomic.LoadInt32(&reqCount) != 1 {
		t.Errorf("expected 1 request, got %d", reqCount)
	}

	// Second call -> returns cached token
	token2, err := src.Token(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token2 != "mock-access-token-123" {
		t.Errorf("expected token mock-access-token-123, got %s", token2)
	}
	if atomic.LoadInt32(&reqCount) != 1 {
		t.Errorf("expected cached token without new request, got %d", reqCount)
	}
}

func TestOAuthTokenSource_FromJSONVariants(t *testing.T) {
	// Snake case flat
	flatJSON := `{"client_id":"c1","client_secret":"s1","refresh_token":"r1"}`
	src1, err := NewOAuthTokenSourceFromJSON([]byte(flatJSON), "")
	if err != nil || src1.clientID != "c1" || src1.refreshToken != "r1" {
		t.Fatalf("flat json failed: %v, src: %+v", err, src1)
	}

	// Camel case flat
	camelJSON := `{"clientId":"c2","clientSecret":"s2","refreshToken":"r2"}`
	src2, err := NewOAuthTokenSourceFromJSON([]byte(camelJSON), "")
	if err != nil || src2.clientID != "c2" || src2.clientSecret != "s2" || src2.refreshToken != "r2" {
		t.Fatalf("camel json failed: %v, src: %+v", err, src2)
	}

	// Google installed app client_secret.json format with overrideRefreshToken
	installedJSON := `{"installed":{"client_id":"c3","client_secret":"s3","token_uri":"https://oauth2.googleapis.com/token"}}`
	src3, err := NewOAuthTokenSourceFromJSON([]byte(installedJSON), "r3-override")
	if err != nil || src3.clientID != "c3" || src3.clientSecret != "s3" || src3.refreshToken != "r3-override" {
		t.Fatalf("installed json failed: %v, src: %+v", err, src3)
	}

	// Google web app client_secret.json format
	webJSON := `{"web":{"client_id":"c4","client_secret":"s4"},"refresh_token":"r4"}`
	src4, err := NewOAuthTokenSourceFromJSON([]byte(webJSON), "")
	if err != nil || src4.clientID != "c4" || src4.clientSecret != "s4" || src4.refreshToken != "r4" {
		t.Fatalf("web json failed: %v, src: %+v", err, src4)
	}
}

func TestOAuthTokenSource_FromFile(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "gdrive_oauth.json")
	content := `{"client_id":"f_client","client_secret":"f_secret","refresh_token":"f_token"}`
	if err := os.WriteFile(filePath, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	src, err := NewOAuthTokenSourceFromFile(filePath, "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if src.clientID != "f_client" || src.clientSecret != "f_secret" || src.refreshToken != "f_token" {
		t.Errorf("mismatched parsed values: %+v", src)
	}
}

func generateTestRSAPrivateKeyPEM(t *testing.T) string {
	t.Helper()
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}

	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		t.Fatalf("marshal pkcs8: %v", err)
	}

	pemBlock := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkcs8Bytes,
	})
	return string(pemBlock)
}

func TestServiceAccountTokenSource(t *testing.T) {
	pemKey := generateTestRSAPrivateKeyPEM(t)

	var reqCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqCount, 1)

		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form err: %v", err)
		}

		if r.Form.Get("grant_type") != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
			t.Errorf("expected jwt grant type, got %s", r.Form.Get("grant_type"))
		}
		if r.Form.Get("assertion") == "" {
			t.Errorf("missing assertion parameter")
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "sa-access-token-999",
			"expires_in":   3600,
			"token_type":   "Bearer",
		})
	}))
	defer server.Close()

	saJSON := fmt.Sprintf(`{
		"type": "service_account",
		"project_id": "test-project",
		"client_email": "sa@test-project.iam.gserviceaccount.com",
		"private_key": %q,
		"token_uri": %q
	}`, pemKey, server.URL)

	src, err := NewServiceAccountTokenSourceFromJSON([]byte(saJSON))
	if err != nil {
		t.Fatalf("failed to create sa token source: %v", err)
	}

	ctx := context.Background()

	// First call -> exchange JWT for access token
	token, err := src.Token(ctx)
	if err != nil {
		t.Fatalf("failed to get token: %v", err)
	}
	if token != "sa-access-token-999" {
		t.Errorf("expected sa-access-token-999, got %s", token)
	}
	if atomic.LoadInt32(&reqCount) != 1 {
		t.Errorf("expected 1 request, got %d", reqCount)
	}

	// Second call -> cached
	token2, err := src.Token(ctx)
	if err != nil {
		t.Fatalf("failed to get token: %v", err)
	}
	if token2 != "sa-access-token-999" {
		t.Errorf("expected sa-access-token-999, got %s", token2)
	}
	if atomic.LoadInt32(&reqCount) != 1 {
		t.Errorf("expected cached token, got %d requests", reqCount)
	}
}

func TestNewDriver_AutoAuthDetection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "auto-detected-token",
			"expires_in":   3600,
		})
	}))
	defer server.Close()

	// 1. OAuth fields
	cfg := Config{
		FolderID:     "folder-123",
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		RefreshToken: "refresh-token",
		TokenURI:     server.URL,
	}

	driver, err := NewDriver(cfg)
	if err != nil {
		t.Fatalf("unexpected driver creation error: %v", err)
	}
	if driver.cfg.TokenFunc == nil {
		t.Fatal("expected TokenFunc to be auto-initialized")
	}

	tok, err := driver.cfg.TokenFunc(context.Background())
	if err != nil {
		t.Fatalf("tokenfunc call failed: %v", err)
	}
	if tok != "auto-detected-token" {
		t.Errorf("expected auto-detected-token, got %s", tok)
	}

	// 2. Service account JSON
	pemKey := generateTestRSAPrivateKeyPEM(t)
	saJSON := fmt.Sprintf(`{
		"client_email": "sa@proj.iam.gserviceaccount.com",
		"private_key": %q,
		"token_uri": %q
	}`, pemKey, server.URL)

	cfgSA := Config{
		FolderID:           "folder-123",
		ServiceAccountJSON: []byte(saJSON),
	}
	driverSA, err := NewDriver(cfgSA)
	if err != nil {
		t.Fatalf("unexpected sa driver creation error: %v", err)
	}
	if driverSA.cfg.TokenFunc == nil {
		t.Fatal("expected SA TokenFunc to be auto-initialized")
	}
	tokSA, err := driverSA.cfg.TokenFunc(context.Background())
	if err != nil {
		t.Fatalf("sa tokenfunc call failed: %v", err)
	}
	if tokSA != "auto-detected-token" {
		t.Errorf("expected auto-detected-token, got %s", tokSA)
	}
}
