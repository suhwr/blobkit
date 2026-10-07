package sftp

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/pkg/sftp"
	"github.com/suhwr/blobkit"
	"golang.org/x/crypto/ssh"
)

// mockSSHServer represents an in-process SSH and SFTP server for hermetic testing.
type mockSSHServer struct {
	listener net.Listener
	host     string
	port     int
	hostKey  ssh.PublicKey
	rootDir  string
	stopChan chan struct{}
}

var (
	testHostSigner ssh.Signer
	testSignerOnce sync.Once
)

func getTestHostSigner(t *testing.T) ssh.Signer {
	testSignerOnce.Do(func() {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			panic(err)
		}
		signer, err := ssh.NewSignerFromKey(key)
		if err != nil {
			panic(err)
		}
		testHostSigner = signer
	})
	return testHostSigner
}

func startMockSSHServer(t *testing.T, user, password string) *mockSSHServer {
	t.Helper()

	signer := getTestHostSigner(t)

	sshConfig := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if c.User() == user && string(pass) == password {
				return nil, nil
			}
			return nil, fmt.Errorf("password rejected")
		},
	}
	sshConfig.AddHostKey(signer)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on loopback: %v", err)
	}

	host, portStr, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portStr)
	rootDir := t.TempDir()
	stopChan := make(chan struct{})

	srv := &mockSSHServer{
		listener: listener,
		host:     host,
		port:     port,
		hostKey:  signer.PublicKey(),
		rootDir:  rootDir,
		stopChan: stopChan,
	}

	go func() {
		for {
			nConn, err := listener.Accept()
			if err != nil {
				select {
				case <-stopChan:
					return
				default:
					return
				}
			}

			go func(c net.Conn) {
				defer c.Close()
				_, chans, reqs, err := ssh.NewServerConn(c, sshConfig)
				if err != nil {
					return
				}
				go ssh.DiscardRequests(reqs)

				for newChannel := range chans {
					if newChannel.ChannelType() != "session" {
						_ = newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
						continue
					}
					channel, requests, err := newChannel.Accept()
					if err != nil {
						continue
					}

					go func(in <-chan *ssh.Request) {
						for req := range in {
							ok := false
							switch req.Type {
							case "subsystem":
								if len(req.Payload) >= 4 && string(req.Payload[4:]) == "sftp" {
									ok = true
								}
							}
							_ = req.Reply(ok, nil)
						}
					}(requests)

					server, err := sftp.NewServer(channel)
					if err != nil {
						continue
					}
					_ = server.Serve()
					_ = server.Close()
				}
			}(nConn)
		}
	}()

	return srv
}

func (s *mockSSHServer) close() {
	close(s.stopChan)
	_ = s.listener.Close()
}

func setupTestSFTPDriver(t *testing.T) (*Driver, *mockSSHServer) {
	t.Helper()
	user := "testuser"
	pass := "testpass"

	srv := startMockSSHServer(t, user, pass)

	cfg := Config{
		Name:                  "sftp-test",
		Host:                  srv.host,
		Port:                  srv.port,
		User:                  user,
		Password:              pass,
		InsecureIgnoreHostKey: true,
		BaseDir:               srv.rootDir,
		EnableSidecarMeta:     true,
		PublicBaseURL:         "https://cdn.example.com",
	}

	driver, err := NewDriver(cfg)
	if err != nil {
		srv.close()
		t.Fatalf("failed to create driver: %v", err)
	}

	return driver, srv
}

func TestConfig_Validation(t *testing.T) {
	// 1. Missing host
	cfg := Config{
		User: "user",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on missing host")
	}

	// 2. Missing user
	cfg = Config{
		Host: "localhost",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on missing user")
	}

	// 3. Missing auth
	cfg = Config{
		Host: "localhost",
		User: "user",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on missing auth")
	}

	// 4. Missing host key check
	cfg = Config{
		Host:     "localhost",
		User:     "user",
		Password: "pass",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on missing HostKey when InsecureIgnoreHostKey is false")
	}

	// 5. Valid config with defaults
	cfg.InsecureIgnoreHostKey = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Port != DefaultPort {
		t.Errorf("expected port %d, got %d", DefaultPort, cfg.Port)
	}
	if cfg.Name != "sftp" {
		t.Errorf("expected name sftp, got %s", cfg.Name)
	}
}

func TestDriver_Capabilities(t *testing.T) {
	driver, srv := setupTestSFTPDriver(t)
	defer srv.close()
	defer driver.Close()

	ctx := context.Background()
	payload := []byte("sftp capability behavioral execution")

	// 1. Behavioral execution of CapDirectPut
	obj, err := driver.Put(ctx, &blobkit.Object{Key: "cap-sftp.txt"}, bytes.NewReader(payload), blobkit.PutOptions{Size: int64(len(payload))})
	if err != nil {
		t.Fatalf("CapDirectPut execution failed: %v", err)
	}
	if obj.Size != int64(len(payload)) {
		t.Fatalf("CapDirectPut size mismatch: %d", obj.Size)
	}

	// 2. Behavioral execution of CapByteRangeGet
	rReader, err := driver.Get(ctx, "cap-sftp.txt", blobkit.GetOptions{Range: "bytes=0-3"})
	if err != nil {
		t.Fatalf("CapByteRangeGet execution failed: %v", err)
	}
	sub, _ := io.ReadAll(rReader)
	rReader.Close()
	if string(sub) != "sftp" {
		t.Fatalf("CapByteRangeGet slice mismatch: %q", string(sub))
	}

	// 3. Behavioral execution of CapCopy
	if err := driver.Copy(ctx, "cap-sftp.txt", "cap-sftp-copy.txt"); err != nil {
		t.Fatalf("CapCopy execution failed: %v", err)
	}
	copyHead, err := driver.Head(ctx, "cap-sftp-copy.txt")
	if err != nil || copyHead.Size != int64(len(payload)) {
		t.Fatalf("CapCopy destination verification failed: %v", err)
	}

	// 4. Negative path: Unadvertised CapPresignGet MUST return ErrUnsupportedOperation
	_, pErr := driver.PresignGet(ctx, "cap-sftp.txt", blobkit.PresignOptions{})
	if !errors.Is(pErr, blobkit.ErrUnsupportedOperation) {
		t.Fatalf("expected ErrUnsupportedOperation for unadvertised PresignGet, got: %v", pErr)
	}
}

func TestDriver_CRUD_And_ByteRange(t *testing.T) {
	driver, srv := setupTestSFTPDriver(t)
	defer srv.close()
	defer driver.Close()

	ctx := context.Background()
	key := "docs/readme.txt"
	content := []byte("Hello SFTP Storage World!")

	// 1. Put
	putObj, err := driver.Put(ctx, &blobkit.Object{
		Key:         key,
		ContentType: "text/plain",
	}, bytes.NewReader(content), blobkit.PutOptions{
		Size: int64(len(content)),
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if putObj.Key != key || putObj.Size != int64(len(content)) {
		t.Fatalf("unexpected put object: %+v", putObj)
	}

	// 2. Head
	headObj, err := driver.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if headObj.Key != key || headObj.Size != int64(len(content)) {
		t.Fatalf("unexpected head object: %+v", headObj)
	}

	// 3. Get Full
	reader, err := driver.Get(ctx, key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if string(data) != string(content) {
		t.Fatalf("expected %q, got %q", string(content), string(data))
	}

	// 4. Get Byte Range (bytes=0-4 -> "Hello")
	rangeReader, err := driver.Get(ctx, key, blobkit.GetOptions{
		Range: "bytes=0-4",
	})
	if err != nil {
		t.Fatalf("Get range failed: %v", err)
	}
	defer rangeReader.Close()

	rangeData, err := io.ReadAll(rangeReader)
	if err != nil {
		t.Fatalf("failed to read range body: %v", err)
	}
	if string(rangeData) != "Hello" {
		t.Fatalf("expected 'Hello', got %q", string(rangeData))
	}

	// 5. Delete
	if err := driver.Delete(ctx, key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 6. Head 404
	_, err = driver.Head(ctx, key)
	if err == nil || !blobkit.IsNotFound(err) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}
}

func TestDriver_SidecarMetadata(t *testing.T) {
	driver, srv := setupTestSFTPDriver(t)
	defer srv.close()
	defer driver.Close()

	ctx := context.Background()
	key := "data/records.csv"
	content := []byte("id,val\n1,100")
	meta := map[string]string{
		"owner":  "accounting",
		"tier":   "archive",
		"status": "audited",
	}

	putObj, err := driver.Put(ctx, &blobkit.Object{
		ID:          "custom-uuid-1234",
		Key:         key,
		ContentType: "text/csv",
	}, bytes.NewReader(content), blobkit.PutOptions{
		Size:     int64(len(content)),
		Metadata: meta,
	})
	if err != nil {
		t.Fatalf("Put with metadata failed: %v", err)
	}
	if putObj.Metadata["owner"] != "accounting" {
		t.Fatalf("expected owner accounting, got %v", putObj.Metadata)
	}

	// Verify sidecar file actually exists on remote filesystem
	sidecarDiskPath := filepath.Join(srv.rootDir, "data", "records.csv.meta.json")
	if _, err := os.Stat(sidecarDiskPath); err != nil {
		t.Fatalf("expected sidecar file on disk at %s: %v", sidecarDiskPath, err)
	}

	// Head loads sidecar
	headObj, err := driver.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if headObj.ContentType != "text/csv" {
		t.Fatalf("expected text/csv, got %s", headObj.ContentType)
	}
	if headObj.Metadata["tier"] != "archive" {
		t.Fatalf("expected tier archive, got %v", headObj.Metadata)
	}
	if headObj.ID != "custom-uuid-1234" {
		t.Fatalf("expected ID custom-uuid-1234, got %s", headObj.ID)
	}
}

func TestDriver_PathTraversalDefense(t *testing.T) {
	driver, srv := setupTestSFTPDriver(t)
	defer srv.close()
	defer driver.Close()

	ctx := context.Background()
	traversalKeys := []string{
		"../outside.txt",
		"../../etc/passwd",
		"sub/../../escape.bin",
		`windows\path\escape.txt`,
	}

	for _, k := range traversalKeys {
		_, err := driver.Put(ctx, &blobkit.Object{Key: k}, bytes.NewReader([]byte("hack")), blobkit.PutOptions{})
		if err == nil || !errors.Is(err, blobkit.ErrSecurityViolation) {
			t.Errorf("expected ErrSecurityViolation for key %q, got: %v", k, err)
		}
	}
}

func TestDriver_Copy_And_BatchDelete(t *testing.T) {
	driver, srv := setupTestSFTPDriver(t)
	defer srv.close()
	defer driver.Close()

	ctx := context.Background()
	srcKey := "source/file.bin"
	dstKey := "backup/file.bin"
	payload := []byte("binary payload for copy")

	_, err := driver.Put(ctx, &blobkit.Object{
		Key:         srcKey,
		ContentType: "application/octet-stream",
	}, bytes.NewReader(payload), blobkit.PutOptions{
		Metadata: map[string]string{"copy-test": "true"},
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Copy
	if err := driver.Copy(ctx, srcKey, dstKey); err != nil {
		t.Fatalf("Copy failed: %v", err)
	}

	// Verify copied object
	dstHead, err := driver.Head(ctx, dstKey)
	if err != nil {
		t.Fatalf("Head dst failed: %v", err)
	}
	if dstHead.Size != int64(len(payload)) {
		t.Fatalf("expected size %d, got %d", len(payload), dstHead.Size)
	}
	if dstHead.Metadata["copy-test"] != "true" {
		t.Fatalf("expected copied metadata, got %v", dstHead.Metadata)
	}

	// Batch Delete
	deleted, err := driver.DeleteBatch(ctx, []string{srcKey, dstKey})
	if err != nil {
		t.Fatalf("DeleteBatch failed: %v", err)
	}
	if len(deleted) != 2 {
		t.Fatalf("expected 2 deleted items, got %d", len(deleted))
	}

	// Both should be gone
	_, err = driver.Head(ctx, srcKey)
	if !blobkit.IsNotFound(err) {
		t.Errorf("expected src not found, got %v", err)
	}
	_, err = driver.Head(ctx, dstKey)
	if !blobkit.IsNotFound(err) {
		t.Errorf("expected dst not found, got %v", err)
	}
}

func TestDriver_List(t *testing.T) {
	driver, srv := setupTestSFTPDriver(t)
	defer srv.close()
	defer driver.Close()

	ctx := context.Background()
	keys := []string{
		"media/photos/img1.png",
		"media/photos/img2.png",
		"media/videos/clip.mp4",
		"root.txt",
	}

	for _, k := range keys {
		_, _ = driver.Put(ctx, &blobkit.Object{Key: k}, bytes.NewReader([]byte("sample")), blobkit.PutOptions{})
	}

	// 1. List with prefix
	res, err := driver.List(ctx, blobkit.ListOptions{
		Prefix: "media/photos/",
	})
	if err != nil {
		t.Fatalf("List prefix failed: %v", err)
	}
	if len(res.Objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(res.Objects))
	}

	// 2. List with delimiter
	delRes, err := driver.List(ctx, blobkit.ListOptions{
		Prefix:    "media/",
		Delimiter: "/",
	})
	if err != nil {
		t.Fatalf("List delimiter failed: %v", err)
	}
	if len(delRes.CommonPrefixes) != 2 {
		t.Fatalf("expected 2 common prefixes, got %v", delRes.CommonPrefixes)
	}
}

func TestDriver_ErrorScrubbing_And_Mapping(t *testing.T) {
	msg := "failed auth with password=super_secret_password and -----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA...\n-----END RSA PRIVATE KEY-----"
	scrubbed := scrubCredentials(msg)

	if strings.Contains(scrubbed, "super_secret_password") {
		t.Fatalf("password leaked: %s", scrubbed)
	}
	if strings.Contains(scrubbed, "MIIEow") {
		t.Fatalf("private key leaked: %s", scrubbed)
	}
	if !strings.Contains(scrubbed, "[REDACTED]") {
		t.Fatalf("expected [REDACTED] in scrubbed output: %s", scrubbed)
	}
}

func TestDriver_Concurrency(t *testing.T) {
	driver, srv := setupTestSFTPDriver(t)
	defer srv.close()
	defer driver.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	workers := 10

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent/%d.txt", id)
			data := []byte(fmt.Sprintf("worker data %d", id))

			// Put
			_, err := driver.Put(ctx, &blobkit.Object{Key: key}, bytes.NewReader(data), blobkit.PutOptions{
				Size: int64(len(data)),
			})
			if err != nil {
				t.Errorf("worker %d Put failed: %v", id, err)
				return
			}

			// Head
			head, err := driver.Head(ctx, key)
			if err != nil {
				t.Errorf("worker %d Head failed: %v", id, err)
				return
			}
			if head.Size != int64(len(data)) {
				t.Errorf("worker %d size mismatch", id)
			}

			// Get
			r, err := driver.Get(ctx, key, blobkit.GetOptions{})
			if err != nil {
				t.Errorf("worker %d Get failed: %v", id, err)
				return
			}
			defer r.Close()
			readBytes, _ := io.ReadAll(r)
			if string(readBytes) != string(data) {
				t.Errorf("worker %d content mismatch", id)
			}

			// Delete
			if err := driver.Delete(ctx, key); err != nil {
				t.Errorf("worker %d Delete failed: %v", id, err)
			}
		}(i)
	}

	wg.Wait()
}

func TestDriver_IntegrationWithBlobKitClient(t *testing.T) {
	driver, srv := setupTestSFTPDriver(t)
	defer srv.close()
	defer driver.Close()

	client, err := blobkit.New(blobkit.WithDriver(driver))
	if err != nil {
		t.Fatalf("failed to init client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	payload := []byte("Client Integration with Native SFTP Driver")

	// 1. Client.Put
	obj, err := client.Put(ctx, bytes.NewReader(payload), blobkit.PutOptions{
		Namespace: "sftp_client",
		Filename:  "sample.txt",
	})
	if err != nil {
		t.Fatalf("Client.Put failed: %v", err)
	}
	if obj.Provider != "sftp-test" {
		t.Fatalf("expected provider sftp-test, got %s", obj.Provider)
	}
	if obj.Size != int64(len(payload)) {
		t.Fatalf("expected size %d, got %d", len(payload), obj.Size)
	}

	// 2. Client.Get
	r, err := client.Get(ctx, obj.Key, blobkit.GetOptions{})
	if err != nil {
		t.Fatalf("Client.Get failed: %v", err)
	}
	data, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if !bytes.Equal(data, payload) {
		t.Fatal("downloaded bytes mismatch")
	}

	// 3. Client.PermanentDelete
	if err := client.PermanentDelete(ctx, obj.Key); err != nil {
		t.Fatalf("Client.PermanentDelete failed: %v", err)
	}

	// 4. Verify deleted
	_, err = client.Head(ctx, obj.Key)
	if !blobkit.IsNotFound(err) {
		t.Fatalf("expected ErrObjectNotFound, got: %v", err)
	}
}
