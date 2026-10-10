package gdrive

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// DefaultTokenEndpoint is the official Google OAuth 2.0 token endpoint.
const DefaultTokenEndpoint = "https://oauth2.googleapis.com/token"

// OAuthTokenSource manages and caches OAuth2 access tokens refreshed from user OAuth2 credentials.
type OAuthTokenSource struct {
	clientID     string
	clientSecret string
	refreshToken string
	tokenURI     string
	httpClient   *http.Client

	mu          sync.Mutex
	accessToken string
	expiresAt   time.Time
}

// OAuthJSON represents OAuth credentials in JSON format (supports flat snake_case, camelCase, or Google client_secret format).
type OAuthJSON struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	RefreshToken string `json:"refresh_token"`
	TokenURI     string `json:"token_uri"`

	// CamelCase alternatives
	ClientIDAlt     string `json:"clientId"`
	ClientSecretAlt string `json:"clientSecret"`
	RefreshTokenAlt string `json:"refreshToken"`

	// Google installed / web app client_secret.json format
	Installed *struct {
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
		TokenURI     string `json:"token_uri"`
	} `json:"installed"`
	Web *struct {
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
		TokenURI     string `json:"token_uri"`
	} `json:"web"`
}

// NewOAuthTokenSource creates a token source from clientID, clientSecret, and refreshToken.
func NewOAuthTokenSource(clientID, clientSecret, refreshToken string) (*OAuthTokenSource, error) {
	if strings.TrimSpace(clientID) == "" || strings.TrimSpace(clientSecret) == "" || strings.TrimSpace(refreshToken) == "" {
		return nil, errors.New("blobkit/gdrive: client_id, client_secret, and refresh_token are required")
	}

	return &OAuthTokenSource{
		clientID:     strings.TrimSpace(clientID),
		clientSecret: strings.TrimSpace(clientSecret),
		refreshToken: strings.TrimSpace(refreshToken),
		tokenURI:     DefaultTokenEndpoint,
		httpClient:   &http.Client{Timeout: 15 * time.Second},
	}, nil
}

// NewOAuthTokenSourceFromFile loads OAuth credentials from a file path.
func NewOAuthTokenSourceFromFile(path string, overrideRefreshToken string) (*OAuthTokenSource, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("blobkit/gdrive: read oauth file: %w", err)
	}
	return NewOAuthTokenSourceFromJSON(data, overrideRefreshToken)
}

// NewOAuthTokenSourceFromJSON parses OAuth credentials from raw JSON bytes.
func NewOAuthTokenSourceFromJSON(data []byte, overrideRefreshToken string) (*OAuthTokenSource, error) {
	var oauthCfg OAuthJSON
	if err := json.Unmarshal(data, &oauthCfg); err != nil {
		return nil, fmt.Errorf("blobkit/gdrive: parse oauth json: %w", err)
	}

	clientID := oauthCfg.ClientID
	if clientID == "" {
		clientID = oauthCfg.ClientIDAlt
	}
	clientSecret := oauthCfg.ClientSecret
	if clientSecret == "" {
		clientSecret = oauthCfg.ClientSecretAlt
	}
	tokenURI := oauthCfg.TokenURI

	if oauthCfg.Installed != nil {
		if clientID == "" {
			clientID = oauthCfg.Installed.ClientID
		}
		if clientSecret == "" {
			clientSecret = oauthCfg.Installed.ClientSecret
		}
		if tokenURI == "" {
			tokenURI = oauthCfg.Installed.TokenURI
		}
	} else if oauthCfg.Web != nil {
		if clientID == "" {
			clientID = oauthCfg.Web.ClientID
		}
		if clientSecret == "" {
			clientSecret = oauthCfg.Web.ClientSecret
		}
		if tokenURI == "" {
			tokenURI = oauthCfg.Web.TokenURI
		}
	}

	refreshToken := oauthCfg.RefreshToken
	if refreshToken == "" {
		refreshToken = oauthCfg.RefreshTokenAlt
	}
	if overrideRefreshToken != "" {
		refreshToken = overrideRefreshToken
	}

	if strings.TrimSpace(clientID) == "" || strings.TrimSpace(clientSecret) == "" || strings.TrimSpace(refreshToken) == "" {
		return nil, errors.New("blobkit/gdrive: invalid oauth json: missing client_id, client_secret, or refresh_token")
	}

	if tokenURI == "" {
		tokenURI = DefaultTokenEndpoint
	}

	return &OAuthTokenSource{
		clientID:     strings.TrimSpace(clientID),
		clientSecret: strings.TrimSpace(clientSecret),
		refreshToken: strings.TrimSpace(refreshToken),
		tokenURI:     tokenURI,
		httpClient:   &http.Client{Timeout: 15 * time.Second},
	}, nil
}

// SetTokenURI sets a custom token URI (primarily for unit testing).
func (s *OAuthTokenSource) SetTokenURI(uri string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokenURI = uri
}

// SetHTTPClient sets a custom HTTP client (primarily for unit testing).
func (s *OAuthTokenSource) SetHTTPClient(client *http.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.httpClient = client
}

// Token returns a valid OAuth2 bearer access token, refreshing it automatically if expired or nearing expiration.
func (s *OAuthTokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Return cached token if valid for at least another 2 minutes
	if s.accessToken != "" && time.Now().Before(s.expiresAt.Add(-2*time.Minute)) {
		return s.accessToken, nil
	}

	now := time.Now()

	form := url.Values{}
	form.Set("client_id", s.clientID)
	form.Set("client_secret", s.clientSecret)
	form.Set("refresh_token", s.refreshToken)
	form.Set("grant_type", "refresh_token")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.tokenURI, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("create oauth refresh request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("exchange oauth refresh token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp struct {
			Error            string `json:"error"`
			ErrorDescription string `json:"error_description"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		return "", fmt.Errorf("oauth token refresh failed (%d): %s - %s", resp.StatusCode, errResp.Error, errResp.ErrorDescription)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", fmt.Errorf("decode oauth token response: %w", err)
	}

	if tokenResp.AccessToken == "" {
		return "", errors.New("oauth token response missing access_token")
	}

	s.accessToken = tokenResp.AccessToken
	if tokenResp.ExpiresIn > 0 {
		s.expiresAt = now.Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
	} else {
		s.expiresAt = now.Add(time.Hour)
	}

	return s.accessToken, nil
}

type serviceAccountJSON struct {
	Type        string `json:"type"`
	ProjectID   string `json:"project_id"`
	PrivateKey  string `json:"private_key"`
	ClientEmail string `json:"client_email"`
	TokenURI    string `json:"token_uri"`
}

// ServiceAccountTokenSource generates and caches OAuth2 access tokens from a Google Cloud Service Account JSON key.
type ServiceAccountTokenSource struct {
	clientEmail string
	tokenURI    string
	privateKey  *rsa.PrivateKey
	httpClient  *http.Client

	mu          sync.Mutex
	accessToken string
	expiresAt   time.Time
}

// NewServiceAccountTokenSourceFromFile creates a token source from a service account JSON file path.
func NewServiceAccountTokenSourceFromFile(path string) (*ServiceAccountTokenSource, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("blobkit/gdrive: read service account file: %w", err)
	}
	return NewServiceAccountTokenSourceFromJSON(data)
}

// NewServiceAccountTokenSourceFromJSON creates a token source from raw service account JSON bytes.
func NewServiceAccountTokenSourceFromJSON(data []byte) (*ServiceAccountTokenSource, error) {
	var sa serviceAccountJSON
	if err := json.Unmarshal(data, &sa); err != nil {
		return nil, fmt.Errorf("blobkit/gdrive: parse service account json: %w", err)
	}

	if sa.ClientEmail == "" || sa.PrivateKey == "" {
		return nil, errors.New("blobkit/gdrive: invalid service account json: missing client_email or private_key")
	}

	tokenURI := sa.TokenURI
	if tokenURI == "" {
		tokenURI = DefaultTokenEndpoint
	}

	privKey, err := parseRSAPrivateKey(sa.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("blobkit/gdrive: parse rsa private key: %w", err)
	}

	return &ServiceAccountTokenSource{
		clientEmail: sa.ClientEmail,
		tokenURI:    tokenURI,
		privateKey:  privKey,
		httpClient:  &http.Client{Timeout: 15 * time.Second},
	}, nil
}

func parseRSAPrivateKey(keyPEM string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(keyPEM))
	if block == nil {
		return nil, errors.New("failed to decode PEM block")
	}

	// 1. Try PKCS#8 (standard for Google Cloud Service Accounts)
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		if rsaKey, ok := key.(*rsa.PrivateKey); ok {
			return rsaKey, nil
		}
		return nil, errors.New("pkcs8 key is not an rsa private key")
	}

	// 2. Try PKCS#1
	if rsaKey, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return rsaKey, nil
	}

	return nil, errors.New("could not parse private key as PKCS#8 or PKCS#1")
}

// SetTokenURI sets a custom token URI (primarily for unit testing).
func (s *ServiceAccountTokenSource) SetTokenURI(uri string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokenURI = uri
}

// SetHTTPClient sets a custom HTTP client (primarily for unit testing).
func (s *ServiceAccountTokenSource) SetHTTPClient(client *http.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.httpClient = client
}

// Token returns a valid OAuth2 bearer access token, refreshing it automatically if expired or nearing expiration.
func (s *ServiceAccountTokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Return cached token if valid for at least another 2 minutes
	if s.accessToken != "" && time.Now().Before(s.expiresAt.Add(-2*time.Minute)) {
		return s.accessToken, nil
	}

	now := time.Now()
	exp := now.Add(time.Hour)

	// Build JWT header and claim set (RFC 7523)
	headerJSON := `{"alg":"RS256","typ":"JWT"}`
	headerB64 := base64.RawURLEncoding.EncodeToString([]byte(headerJSON))

	claimJSON := fmt.Sprintf(
		`{"iss":%q,"scope":"https://www.googleapis.com/auth/drive","aud":%q,"exp":%d,"iat":%d}`,
		s.clientEmail, s.tokenURI, exp.Unix(), now.Unix(),
	)
	claimB64 := base64.RawURLEncoding.EncodeToString([]byte(claimJSON))

	signingInput := headerB64 + "." + claimB64
	h := sha256.New()
	h.Write([]byte(signingInput))
	digest := h.Sum(nil)

	sig, err := rsa.SignPKCS1v15(rand.Reader, s.privateKey, crypto.SHA256, digest)
	if err != nil {
		return "", fmt.Errorf("sign jwt assertion: %w", err)
	}

	signedAssertion := signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)

	// Exchange signed assertion for access token
	form := url.Values{}
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	form.Set("assertion", signedAssertion)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.tokenURI, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("exchange token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp struct {
			Error            string `json:"error"`
			ErrorDescription string `json:"error_description"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		return "", fmt.Errorf("token exchange failed (%d): %s - %s", resp.StatusCode, errResp.Error, errResp.ErrorDescription)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}

	if tokenResp.AccessToken == "" {
		return "", errors.New("token response missing access_token")
	}

	s.accessToken = tokenResp.AccessToken
	if tokenResp.ExpiresIn > 0 {
		s.expiresAt = now.Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
	} else {
		s.expiresAt = now.Add(time.Hour)
	}

	return s.accessToken, nil
}
