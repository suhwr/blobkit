# Declarative Multi-Cloud Fleet Routing

The `router/fleet` package provides a declarative configuration loader and orchestrator for multi-cloud, multi-account, and heterogeneous storage topologies. It unifies all 8 BlobKit storage drivers under a single declarative specification (JSON or YAML), auto-wiring complex routing rules, failover pairs, and circuit breakers.

---

## Key Capabilities & Highlights

| Capability | Supported | Description |
| :--- | :---: | :--- |
| **Declarative JSON/YAML** | Yes | Load entire storage fleets from configuration files or Kubernetes ConfigMaps via `fleet.LoadJSONFile(...)`. |
| **Heterogeneous Fleets** | Yes | Mix and match S3, Azure, GCS, WebDAV, Google Drive, SFTP, and Local Filesystem under one client. |
| **Namespace Routing** | Yes | Route uploads automatically by namespace/folder (e.g. `avatars` $\rightarrow$ Cloudflare R2, `compliance` $\rightarrow$ Wasabi). |
| **Active-Passive Failover** | Yes | Automatically demotes unhealthy providers and fails over to secondary replicas upon consecutive errors. |
| **3-State Circuit Breakers** | Yes | Trips to fallback drivers on 5xx cloud outages; executes half-open probes to restore traffic automatically. |
| **Weighted Traffic Splitting**| Yes | Distributes traffic proportionally across providers for multi-cloud load balancing and blue-green canaries. |
| **Automated Teardown** | Yes | Gracefully terminates all underlying driver pools and worker threads on `fleet.Close()`. |

---

## Routing Topologies

### 1. Namespace Routing (`strategy: "namespace"`)
Routes incoming payloads to specific cloud backends based on their `Namespace` or key prefix:
- `media/*` $\rightarrow$ **Cloudflare R2** (Zero egress bandwidth fees).
- `documents/*` $\rightarrow$ **AWS S3** (High durability and versioning).
- `archives/*` $\rightarrow$ **Wasabi** (Low-cost long-term compliance storage).
- `default` $\rightarrow$ **Local Filesystem** (Default fallback).

### 2. 3-State Circuit Breaker (`strategy: "circuit_breaker"`)
Protects mission-critical applications from cloud outages:
- **CLOSED**: Traffic flows normally to the primary provider.
- **OPEN**: When errors reach `failure_threshold`, traffic immediately shifts to the fallback provider without waiting for network timeouts.
- **HALF-OPEN**: After `cooldown` period, a probe request tests the primary provider. If successful, the circuit resets to CLOSED.

### 3. Active-Passive Failover (`strategy: "failover"`)
Directs 100% of traffic to the primary driver. If the primary accumulates `max_consecutive_failures`, it fails over to the secondary replica.

### 4. Weighted Traffic Splitting (`strategy: "weighted"`)
Distributes requests across providers based on configured percentage weights (e.g. AWS 80%, GCP 20%).

---

## Declarative JSON Configuration Schema

```json
{
  "providers": [
    {
      "name": "r2-public",
      "type": "s3",
      "s3": {
        "bucket": "media-production",
        "region": "auto",
        "endpoint": "https://<ACCOUNT_ID>.r2.cloudflarestorage.com",
        "access_key": "<R2_ACCESS_KEY>",
        "secret_key": "<R2_SECRET_KEY>",
        "public_base_url": "https://cdn.example.com"
      }
    },
    {
      "name": "aws-cold-archive",
      "type": "s3",
      "s3": {
        "bucket": "cold-storage",
        "region": "us-east-1",
        "access_key": "<AWS_ACCESS_KEY>",
        "secret_key": "<AWS_SECRET_KEY>"
      }
    },
    {
      "name": "azure-compliance",
      "type": "azure",
      "azure": {
        "account_name": "corpcompliance",
        "account_key": "<AZURE_KEY>",
        "container": "legal-records"
      }
    },
    {
      "name": "local-fallback",
      "type": "fs",
      "fs": {
        "root_dir": "/var/data/emergency_blobs",
        "enable_sidecar_meta": true
      }
    }
  ],
  "routing": {
    "strategy": "namespace",
    "default_provider": "r2-public",
    "namespaces": {
      "media": "r2-public",
      "legal": "azure-compliance",
      "archive": "aws-cold-archive"
    }
  }
}
```

---

## Loading & Initializing in Go

### Loading from File

```go
package main

import (
    "context"
    "fmt"
    "strings"

    "github.com/suhwr/blobkit"
    "github.com/suhwr/blobkit/router/fleet"
)

func main() {
    ctx := context.Background()

    // Load declarative topology from file
    fl, err := fleet.LoadJSONFile("config/storage_fleet.json")
    if err != nil {
        panic(err)
    }
    defer fl.Close()

    // Initialize BlobKit client with the fleet router
    client, err := blobkit.New(blobkit.WithRouter(fl.Router()))
    if err != nil {
        panic(err)
    }

    // Automatically routed to "azure-compliance" based on namespace:
    legalDoc, err := client.Put(ctx, strings.NewReader("Board Meeting Minutes"), blobkit.PutOptions{
        Namespace: "legal",
        Filename:  "minutes_2026.pdf",
    })
    if err != nil {
        panic(err)
    }
    fmt.Println("Legal Doc Stored with Provider:", legalDoc.Provider) // "azure-compliance"

    // Automatically routed to "r2-public" based on namespace:
    avatar, err := client.Put(ctx, strings.NewReader("PNG image data..."), blobkit.PutOptions{
        Namespace: "media",
        Filename:  "user_avatar.png",
    })
    if err != nil {
        panic(err)
    }
    fmt.Println("Media Stored with Provider:", avatar.Provider) // "r2-public"
}
```

### Loading In-Code with Go Structs

```go
cfg := fleet.FleetConfig{
    Providers: []fleet.ProviderConfig{
        {
            Type: fleet.ProviderTypeS3,
            Name: "s3-primary",
            S3: &s3.Config{
                Bucket:    "primary-bucket",
                Region:    "us-east-1",
                AccessKey: "...",
                SecretKey: "...",
            },
        },
        {
            Type: fleet.ProviderTypeAzure,
            Name: "azure-fallback",
            Azure: &azure.Config{
                AccountName: "...",
                AccountKey:  "...",
                Container:   "fallback-container",
            },
        },
    },
    Routing: fleet.RoutingConfig{
        Strategy: fleet.StrategyCircuitBreaker,
        CircuitBreaker: &fleet.CircuitConfig{
            Primary:          "s3-primary",
            Fallback:         "azure-fallback",
            FailureThreshold: 5,
            SuccessThreshold: 2,
            Cooldown:         30 * time.Second,
        },
    },
}

fl, err := fleet.Load(cfg)
if err != nil {
    panic(err)
}
defer fl.Close()
```
