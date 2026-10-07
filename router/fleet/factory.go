package fleet

import (
	"fmt"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/azure"
	"github.com/suhwr/blobkit/provider/fs"
	"github.com/suhwr/blobkit/provider/gcs"
	"github.com/suhwr/blobkit/provider/gdrive"
	"github.com/suhwr/blobkit/provider/memory"
	"github.com/suhwr/blobkit/provider/s3"
	"github.com/suhwr/blobkit/provider/sftp"
	"github.com/suhwr/blobkit/provider/webdav"
)

// buildDriver dispatches provider configuration to the appropriate driver constructor.
func buildDriver(p ProviderConfig) (blobkit.Driver, error) {
	switch p.Type {
	case ProviderTypeS3:
		cfg := *p.S3
		if cfg.Name == "" {
			cfg.Name = p.Name
		}
		return s3.NewDriver(cfg)

	case ProviderTypeAzure:
		cfg := *p.Azure
		if cfg.Name == "" {
			cfg.Name = p.Name
		}
		return azure.NewDriver(cfg)

	case ProviderTypeGCS:
		cfg := *p.GCS
		if cfg.Name == "" {
			cfg.Name = p.Name
		}
		return gcs.NewDriver(cfg)

	case ProviderTypeWebDAV:
		cfg := *p.WebDAV
		if cfg.Name == "" {
			cfg.Name = p.Name
		}
		return webdav.NewDriver(cfg)

	case ProviderTypeGDrive:
		cfg := *p.GDrive
		if cfg.Name == "" {
			cfg.Name = p.Name
		}
		return gdrive.NewDriver(cfg)

	case ProviderTypeFS:
		cfg := *p.FS
		if cfg.Name == "" {
			cfg.Name = p.Name
		}
		return fs.NewDriver(cfg)

	case ProviderTypeSFTP:
		cfg := *p.SFTP
		if cfg.Name == "" {
			cfg.Name = p.Name
		}
		return sftp.NewDriver(cfg)

	case ProviderTypeMemory:
		cfg := *p.Memory
		if cfg.Name == "" {
			cfg.Name = p.Name
		}
		return memory.NewDriver(cfg), nil

	default:
		return nil, fmt.Errorf("fleet: unsupported provider type %q", p.Type)
	}
}
