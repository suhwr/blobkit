package fleet

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/suhwr/blobkit"
	"github.com/suhwr/blobkit/provider/fs"
	"github.com/suhwr/blobkit/provider/memory"
)

func TestFleet_Validation(t *testing.T) {
	// 1. Empty providers
	cfg := FleetConfig{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on empty providers")
	}

	// 2. Empty provider name
	cfg = FleetConfig{
		Providers: []ProviderConfig{
			{Type: ProviderTypeMemory, Name: ""},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on empty provider name")
	}

	// 3. Duplicate provider name
	cfg = FleetConfig{
		Providers: []ProviderConfig{
			{Type: ProviderTypeMemory, Name: "mem1", Memory: &memory.Config{Name: "mem1"}},
			{Type: ProviderTypeMemory, Name: "mem1", Memory: &memory.Config{Name: "mem1"}},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on duplicate provider name")
	}

	// 4. Missing provider config
	cfg = FleetConfig{
		Providers: []ProviderConfig{
			{Type: ProviderTypeFS, Name: "fs1"}, // FS is nil
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on nil FS configuration")
	}

	// 5. Unknown default provider in Fixed routing
	cfg = FleetConfig{
		Providers: []ProviderConfig{
			{Type: ProviderTypeMemory, Name: "mem1"},
			{Type: ProviderTypeMemory, Name: "mem2"},
		},
		Routing: RoutingConfig{
			Strategy:        StrategyFixed,
			DefaultProvider: "unknown",
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on unknown default provider")
	}

	// 6. Namespace routing undefined target
	cfg = FleetConfig{
		Providers: []ProviderConfig{
			{Type: ProviderTypeMemory, Name: "mem1"},
		},
		Routing: RoutingConfig{
			Strategy: StrategyNamespace,
			Namespaces: map[string]string{
				"avatars": "non-existent",
			},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on undefined provider in namespace routing")
	}

	// 7. Failover routing missing secondary
	cfg = FleetConfig{
		Providers: []ProviderConfig{
			{Type: ProviderTypeMemory, Name: "primary"},
		},
		Routing: RoutingConfig{
			Strategy: StrategyFailover,
			Failover: &FailoverConfig{
				Primary:   "primary",
				Secondary: "missing",
			},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error on missing failover secondary")
	}
}

func TestFleet_FixedRouting(t *testing.T) {
	cfg := FleetConfig{
		Providers: []ProviderConfig{
			{
				Type:   ProviderTypeMemory,
				Name:   "mem-primary",
				Memory: &memory.Config{Name: "mem-primary", Bucket: "b1"},
			},
		},
		Routing: RoutingConfig{
			Strategy: StrategyFixed,
		},
	}

	fleet, err := Load(cfg)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer fleet.Close()

	d, found := fleet.Driver("mem-primary")
	if !found || d == nil {
		t.Fatal("expected to find mem-primary driver")
	}
	if d.Name() != "mem-primary" {
		t.Fatalf("expected driver name mem-primary, got %s", d.Name())
	}

	// Test Route Select
	ctx := context.Background()
	sel, err := fleet.Router().Select(ctx, blobkit.RouteContext{Op: blobkit.OpPut})
	if err != nil {
		t.Fatalf("Router.Select failed: %v", err)
	}
	if sel.Name() != "mem-primary" {
		t.Fatalf("expected selected driver mem-primary, got %s", sel.Name())
	}
}

func TestFleet_NamespaceRouting(t *testing.T) {
	tempDir := t.TempDir()

	cfg := FleetConfig{
		Providers: []ProviderConfig{
			{
				Type:   ProviderTypeMemory,
				Name:   "mem-avatars",
				Memory: &memory.Config{Name: "mem-avatars"},
			},
			{
				Type: ProviderTypeFS,
				Name: "fs-backups",
				FS: &fs.Config{
					Name:    "fs-backups",
					RootDir: tempDir,
				},
			},
		},
		Routing: RoutingConfig{
			Strategy:        StrategyNamespace,
			DefaultProvider: "mem-avatars",
			Namespaces: map[string]string{
				"user/avatars":  "mem-avatars",
				"system/backup": "fs-backups",
			},
		},
	}

	fleet, err := Load(cfg)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer fleet.Close()

	ctx := context.Background()

	// 1. Route to avatars
	d1, err := fleet.Router().Select(ctx, blobkit.RouteContext{Namespace: "user/avatars"})
	if err != nil || d1.Name() != "mem-avatars" {
		t.Fatalf("expected mem-avatars, got %v (err: %v)", d1, err)
	}

	// 2. Route to backups
	d2, err := fleet.Router().Select(ctx, blobkit.RouteContext{Namespace: "system/backup"})
	if err != nil || d2.Name() != "fs-backups" {
		t.Fatalf("expected fs-backups, got %v (err: %v)", d2, err)
	}

	// 3. Fallback to default
	d3, err := fleet.Router().Select(ctx, blobkit.RouteContext{Namespace: "other"})
	if err != nil || d3.Name() != "mem-avatars" {
		t.Fatalf("expected fallback to mem-avatars, got %v", d3)
	}
}

func TestFleet_FailoverRouting(t *testing.T) {
	cfg := FleetConfig{
		Providers: []ProviderConfig{
			{Type: ProviderTypeMemory, Name: "primary", Memory: &memory.Config{Name: "primary"}},
			{Type: ProviderTypeMemory, Name: "backup", Memory: &memory.Config{Name: "backup"}},
		},
		Routing: RoutingConfig{
			Strategy: StrategyFailover,
			Failover: &FailoverConfig{
				Primary:                "primary",
				Secondary:              "backup",
				MaxConsecutiveFailures: 2,
				Cooldown:               1 * time.Minute,
			},
		},
	}

	fleet, err := Load(cfg)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer fleet.Close()

	ctx := context.Background()

	// Initial selection should be primary
	d, err := fleet.Router().Select(ctx, blobkit.RouteContext{Op: blobkit.OpPut})
	if err != nil || d.Name() != "primary" {
		t.Fatalf("expected primary, got %v (err: %v)", d, err)
	}
}

func TestFleet_CircuitBreakerRouting(t *testing.T) {
	cfg := FleetConfig{
		Providers: []ProviderConfig{
			{Type: ProviderTypeMemory, Name: "main", Memory: &memory.Config{Name: "main"}},
			{Type: ProviderTypeMemory, Name: "fallback", Memory: &memory.Config{Name: "fallback"}},
		},
		Routing: RoutingConfig{
			Strategy: StrategyCircuitBreaker,
			CircuitBreaker: &CircuitConfig{
				Primary:          "main",
				Fallback:         "fallback",
				FailureThreshold: 3,
				Cooldown:         30 * time.Second,
			},
		},
	}

	fleet, err := Load(cfg)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer fleet.Close()

	ctx := context.Background()
	d, err := fleet.Router().Select(ctx, blobkit.RouteContext{Op: blobkit.OpPut})
	if err != nil || d.Name() != "main" {
		t.Fatalf("expected main, got %v (err: %v)", d, err)
	}
}

func TestFleet_WeightedRouting(t *testing.T) {
	cfg := FleetConfig{
		Providers: []ProviderConfig{
			{Type: ProviderTypeMemory, Name: "canary", Memory: &memory.Config{Name: "canary"}},
			{Type: ProviderTypeMemory, Name: "stable", Memory: &memory.Config{Name: "stable"}},
		},
		Routing: RoutingConfig{
			Strategy: StrategyWeighted,
			Weights: map[string]int{
				"canary": 10,
				"stable": 90,
			},
		},
	}

	fleet, err := Load(cfg)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	defer fleet.Close()

	ctx := context.Background()
	d, err := fleet.Router().Select(ctx, blobkit.RouteContext{Op: blobkit.OpPut})
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	if d.Name() != "canary" && d.Name() != "stable" {
		t.Fatalf("unexpected selected driver %s", d.Name())
	}
}

func TestFleet_LoadFromJSON_And_ClientIntegration(t *testing.T) {
	jsonConfig := `
{
  "providers": [
    {
      "type": "memory",
      "name": "mem-prod",
      "memory": {
        "name": "mem-prod",
        "bucket": "prod-blobs"
      }
    }
  ],
  "routing": {
    "strategy": "fixed",
    "default_provider": "mem-prod"
  }
}
`

	fleet, err := LoadFromJSON([]byte(jsonConfig))
	if err != nil {
		t.Fatalf("LoadFromJSON failed: %v", err)
	}
	defer fleet.Close()

	// Verify Drivers map
	drivers := fleet.Drivers()
	if len(drivers) != 1 || drivers["mem-prod"] == nil {
		t.Fatalf("unexpected drivers map: %v", drivers)
	}

	// Create client through fleet
	client, err := fleet.NewClient()
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	payload := []byte("Fleet Managed Client Payload")

	// 1. Put
	obj, err := client.Put(ctx, bytes.NewReader(payload), blobkit.PutOptions{
		Namespace: "fleet_test",
		Filename:  "data.txt",
	})
	if err != nil {
		t.Fatalf("client.Put failed: %v", err)
	}
	if obj.Provider != "mem-prod" {
		t.Fatalf("expected provider mem-prod, got %s", obj.Provider)
	}

	// 2. Exists
	exists, err := client.Exists(ctx, obj.Key)
	if err != nil || !exists {
		t.Fatalf("expected object to exist, got %v (err: %v)", exists, err)
	}

	// 3. Delete
	if err := client.PermanentDelete(ctx, obj.Key); err != nil {
		t.Fatalf("client.PermanentDelete failed: %v", err)
	}

	// 4. Verify deleted
	exists, _ = client.Exists(ctx, obj.Key)
	if exists {
		t.Fatal("object still exists after delete")
	}
}

func TestFleet_LoadFromFile(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "fleet.json")

	content := []byte(`{
  "providers": [
    {"type": "memory", "name": "disk-mem", "memory": {"name": "disk-mem"}}
  ],
  "routing": {
    "strategy": "fixed",
    "default_provider": "disk-mem"
  }
}`)

	if err := os.WriteFile(filePath, content, 0644); err != nil {
		t.Fatalf("failed to write test config file: %v", err)
	}

	fleet, err := LoadFromFile(filePath)
	if err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}
	defer fleet.Close()

	if _, ok := fleet.Driver("disk-mem"); !ok {
		t.Fatal("expected disk-mem driver to be present")
	}
}
