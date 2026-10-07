package fleet

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/suhwr/blobkit/provider/azure"
	"github.com/suhwr/blobkit/provider/fs"
	"github.com/suhwr/blobkit/provider/gcs"
	"github.com/suhwr/blobkit/provider/gdrive"
	"github.com/suhwr/blobkit/provider/memory"
	"github.com/suhwr/blobkit/provider/s3"
	"github.com/suhwr/blobkit/provider/sftp"
	"github.com/suhwr/blobkit/provider/webdav"
)

// ProviderType identifies the underlying storage backend technology.
type ProviderType string

const (
	ProviderTypeS3     ProviderType = "s3"
	ProviderTypeAzure  ProviderType = "azure"
	ProviderTypeGCS    ProviderType = "gcs"
	ProviderTypeWebDAV ProviderType = "webdav"
	ProviderTypeGDrive ProviderType = "gdrive"
	ProviderTypeFS     ProviderType = "fs"
	ProviderTypeSFTP   ProviderType = "sftp"
	ProviderTypeMemory ProviderType = "memory"
)

// ProviderConfig specifies the declaration for a single storage driver instance.
type ProviderConfig struct {
	Type   ProviderType   `json:"type" yaml:"type"`
	Name   string         `json:"name" yaml:"name"`
	S3     *s3.Config     `json:"s3,omitempty" yaml:"s3,omitempty"`
	Azure  *azure.Config  `json:"azure,omitempty" yaml:"azure,omitempty"`
	GCS    *gcs.Config    `json:"gcs,omitempty" yaml:"gcs,omitempty"`
	WebDAV *webdav.Config `json:"webdav,omitempty" yaml:"webdav,omitempty"`
	GDrive *gdrive.Config `json:"gdrive,omitempty" yaml:"gdrive,omitempty"`
	FS     *fs.Config     `json:"fs,omitempty" yaml:"fs,omitempty"`
	SFTP   *sftp.Config   `json:"sftp,omitempty" yaml:"sftp,omitempty"`
	Memory *memory.Config `json:"memory,omitempty" yaml:"memory,omitempty"`
}

// RoutingStrategy defines how operations are dispatched among fleet providers.
type RoutingStrategy string

const (
	StrategyFixed          RoutingStrategy = "fixed"
	StrategyNamespace      RoutingStrategy = "namespace"
	StrategyFailover       RoutingStrategy = "failover"
	StrategyCircuitBreaker RoutingStrategy = "circuit_breaker"
	StrategyWeighted       RoutingStrategy = "weighted"
)

// FailoverConfig configures active-passive failover between two providers.
type FailoverConfig struct {
	Primary                string        `json:"primary" yaml:"primary"`
	Secondary              string        `json:"secondary" yaml:"secondary"`
	MaxConsecutiveFailures int           `json:"max_consecutive_failures,omitempty" yaml:"max_consecutive_failures,omitempty"`
	Cooldown               time.Duration `json:"cooldown,omitempty" yaml:"cooldown,omitempty"`
}

// CircuitConfig configures 3-state circuit breaker protection.
type CircuitConfig struct {
	Primary          string        `json:"primary" yaml:"primary"`
	Fallback         string        `json:"fallback" yaml:"fallback"`
	FailureThreshold int           `json:"failure_threshold,omitempty" yaml:"failure_threshold,omitempty"`
	SuccessThreshold int           `json:"success_threshold,omitempty" yaml:"success_threshold,omitempty"`
	Cooldown         time.Duration `json:"cooldown,omitempty" yaml:"cooldown,omitempty"`
}

// RoutingConfig declares the fleet routing topology.
type RoutingConfig struct {
	Strategy        RoutingStrategy   `json:"strategy" yaml:"strategy"`
	DefaultProvider string            `json:"default_provider,omitempty" yaml:"default_provider,omitempty"`
	Namespaces      map[string]string `json:"namespaces,omitempty" yaml:"namespaces,omitempty"`
	Failover        *FailoverConfig   `json:"failover,omitempty" yaml:"failover,omitempty"`
	CircuitBreaker  *CircuitConfig    `json:"circuit_breaker,omitempty" yaml:"circuit_breaker,omitempty"`
	Weights         map[string]int    `json:"weights,omitempty" yaml:"weights,omitempty"`
}

// FleetConfig is the root declarative configuration schema for multi-provider fleets.
type FleetConfig struct {
	Providers []ProviderConfig `json:"providers" yaml:"providers"`
	Routing   RoutingConfig    `json:"routing" yaml:"routing"`
}

// Validate checks FleetConfig for structural validity and semantic coherence.
func (c *FleetConfig) Validate() error {
	if len(c.Providers) == 0 {
		return errors.New("fleet: at least one provider must be defined")
	}

	providerNames := make(map[string]bool)
	for i, p := range c.Providers {
		name := strings.TrimSpace(p.Name)
		if name == "" {
			return fmt.Errorf("fleet: provider at index %d has empty name", i)
		}
		if providerNames[name] {
			return fmt.Errorf("fleet: duplicate provider name %q", name)
		}
		providerNames[name] = true

		switch p.Type {
		case ProviderTypeS3:
			if p.S3 == nil {
				return fmt.Errorf("fleet: provider %q is type %q but has no s3 configuration", name, p.Type)
			}
		case ProviderTypeAzure:
			if p.Azure == nil {
				return fmt.Errorf("fleet: provider %q is type %q but has no azure configuration", name, p.Type)
			}
		case ProviderTypeGCS:
			if p.GCS == nil {
				return fmt.Errorf("fleet: provider %q is type %q but has no gcs configuration", name, p.Type)
			}
		case ProviderTypeWebDAV:
			if p.WebDAV == nil {
				return fmt.Errorf("fleet: provider %q is type %q but has no webdav configuration", name, p.Type)
			}
		case ProviderTypeGDrive:
			if p.GDrive == nil {
				return fmt.Errorf("fleet: provider %q is type %q but has no gdrive configuration", name, p.Type)
			}
		case ProviderTypeFS:
			if p.FS == nil {
				return fmt.Errorf("fleet: provider %q is type %q but has no fs configuration", name, p.Type)
			}
		case ProviderTypeSFTP:
			if p.SFTP == nil {
				return fmt.Errorf("fleet: provider %q is type %q but has no sftp configuration", name, p.Type)
			}
		case ProviderTypeMemory:
			if p.Memory == nil {
				p.Memory = &memory.Config{Name: name}
			}
		default:
			return fmt.Errorf("fleet: unknown provider type %q for provider %q", p.Type, name)
		}
	}

	strategy := c.Routing.Strategy
	if strategy == "" {
		if len(c.Routing.Namespaces) > 0 {
			strategy = StrategyNamespace
		} else if c.Routing.Failover != nil {
			strategy = StrategyFailover
		} else if c.Routing.CircuitBreaker != nil {
			strategy = StrategyCircuitBreaker
		} else if len(c.Routing.Weights) > 0 {
			strategy = StrategyWeighted
		} else {
			strategy = StrategyFixed
		}
		c.Routing.Strategy = strategy
	}

	// Validate routing strategy
	switch strategy {
	case StrategyFixed:
		def := c.Routing.DefaultProvider
		if def == "" && len(c.Providers) == 1 {
			def = c.Providers[0].Name
			c.Routing.DefaultProvider = def
		}
		if def == "" {
			return errors.New("fleet: fixed routing requires default_provider")
		}
		if !providerNames[def] {
			return fmt.Errorf("fleet: default_provider %q is not defined in providers", def)
		}

	case StrategyNamespace:
		if len(c.Routing.Namespaces) == 0 {
			return errors.New("fleet: namespace routing requires at least one namespace mapping")
		}
		for ns, pName := range c.Routing.Namespaces {
			if !providerNames[pName] {
				return fmt.Errorf("fleet: namespace %q references undefined provider %q", ns, pName)
			}
		}
		if c.Routing.DefaultProvider != "" && !providerNames[c.Routing.DefaultProvider] {
			return fmt.Errorf("fleet: default_provider %q is not defined in providers", c.Routing.DefaultProvider)
		}

	case StrategyFailover:
		if c.Routing.Failover == nil {
			return errors.New("fleet: failover routing requires failover configuration")
		}
		if !providerNames[c.Routing.Failover.Primary] {
			return fmt.Errorf("fleet: failover primary %q is not defined in providers", c.Routing.Failover.Primary)
		}
		if !providerNames[c.Routing.Failover.Secondary] {
			return fmt.Errorf("fleet: failover secondary %q is not defined in providers", c.Routing.Failover.Secondary)
		}

	case StrategyCircuitBreaker:
		if c.Routing.CircuitBreaker == nil {
			return errors.New("fleet: circuit_breaker routing requires circuit_breaker configuration")
		}
		if !providerNames[c.Routing.CircuitBreaker.Primary] {
			return fmt.Errorf("fleet: circuit breaker primary %q is not defined in providers", c.Routing.CircuitBreaker.Primary)
		}
		if !providerNames[c.Routing.CircuitBreaker.Fallback] {
			return fmt.Errorf("fleet: circuit breaker fallback %q is not defined in providers", c.Routing.CircuitBreaker.Fallback)
		}

	case StrategyWeighted:
		if len(c.Routing.Weights) == 0 {
			return errors.New("fleet: weighted routing requires weights configuration")
		}
		totalWeight := 0
		for pName, w := range c.Routing.Weights {
			if !providerNames[pName] {
				return fmt.Errorf("fleet: weights references undefined provider %q", pName)
			}
			if w < 0 {
				return fmt.Errorf("fleet: weight for provider %q cannot be negative", pName)
			}
			totalWeight += w
		}
		if totalWeight <= 0 {
			return errors.New("fleet: total weight must be greater than zero")
		}

	default:
		return fmt.Errorf("fleet: unknown routing strategy %q", strategy)
	}

	return nil
}
