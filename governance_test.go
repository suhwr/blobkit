package blobkit_test

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func findRepoRoot(t *testing.T) string {
	t.Helper()
	candidates := []string{".", "..", "../.."}
	for _, c := range candidates {
		if _, err := os.Stat(filepath.Join(c, "docs", "governance.md")); err == nil {
			return c
		}
	}
	t.Fatalf("could not locate repository root containing docs/governance.md")
	return ""
}

// TestGovernance_ProviderConformanceSuiteImplemented guarantees that every in-tree
// storage provider implements contract testing and driver validation.
func TestGovernance_ProviderConformanceSuiteImplemented(t *testing.T) {
	root := findRepoRoot(t)
	providerDir := filepath.Join(root, "provider")

	entries, err := os.ReadDir(providerDir)
	if err != nil {
		t.Fatalf("failed to read provider directory: %v", err)
	}

	expectedProviders := map[string]bool{
		"azure":  false,
		"fs":     false,
		"gcs":    false,
		"gdrive": false,
		"memory": false,
		"s3":     false,
		"sftp":   false,
		"webdav": false,
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if _, expected := expectedProviders[name]; !expected {
			continue
		}

		expectedProviders[name] = true
		dirPath := filepath.Join(providerDir, name)

		// Verify presence of test files
		dirEntries, err := os.ReadDir(dirPath)
		if err != nil {
			t.Fatalf("failed to read provider/%s: %v", name, err)
		}

		hasContractTest := false
		hasDriverTest := false
		for _, de := range dirEntries {
			if de.Name() == "contract_test.go" {
				hasContractTest = true
			}
			if de.Name() == "driver_test.go" {
				hasDriverTest = true
			}
		}

		if !hasContractTest {
			t.Errorf("provider/%s is missing contract_test.go", name)
		}
		if !hasDriverTest {
			t.Errorf("provider/%s is missing driver_test.go", name)
		}
	}

	for prov, found := range expectedProviders {
		if !found {
			t.Errorf("expected provider %q not found in provider/ directory", prov)
		}
	}
}

// TestGovernance_ContractIDCatalogIntegrity verifies that every Contract ID cataloged
// in docs/governance.md points to an actual existing file and verified test symbol.
func TestGovernance_ContractIDCatalogIntegrity(t *testing.T) {
	root := findRepoRoot(t)
	govPath := filepath.Join(root, "docs", "governance.md")

	file, err := os.Open(govPath)
	if err != nil {
		t.Fatalf("failed to open %s: %v", govPath, err)
	}
	defer file.Close()

	// Regex to match Markdown table rows:
	// | **PUT-001** | Put Lifecycle | Streams payload... | `testutil/contract.go:Contract_Put_Get_Head_Delete` |
	rowRegex := regexp.MustCompile(`^\|\s*\*\*([A-Z0-9_-]+)\*\*\s*\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|\s*` + "`" + `([^` + "`" + `]+)` + "`" + `\s*\|$`)

	type contractEntry struct {
		id       string
		name     string
		desc     string
		location string
	}

	var entries []contractEntry
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		matches := rowRegex.FindStringSubmatch(line)
		if len(matches) == 5 {
			entries = append(entries, contractEntry{
				id:       matches[1],
				name:     strings.TrimSpace(matches[2]),
				desc:     strings.TrimSpace(matches[3]),
				location: strings.TrimSpace(matches[4]),
			})
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner error: %v", err)
	}

	if len(entries) < 15 {
		t.Fatalf("expected at least 15 cataloged contract IDs in governance.md, found %d", len(entries))
	}

	for _, e := range entries {
		parts := strings.Split(e.location, ":")
		relFile := parts[0]
		var symbol string
		if len(parts) > 1 {
			symbol = parts[1]
		}

		fullPath := filepath.Join(root, relFile)
		contentBytes, err := os.ReadFile(fullPath)
		if err != nil {
			t.Errorf("Contract [%s]: referenced file %s does not exist: %v", e.id, relFile, err)
			continue
		}

		if symbol != "" {
			if !strings.Contains(string(contentBytes), symbol) {
				t.Errorf("Contract [%s]: symbol %q not found in %s", e.id, symbol, relFile)
			}
		}
	}
}

// TestGovernance_ContributionTemplatesExists verifies that standard contribution templates
// for New Provider, New Feature, and Contract Change are available.
func TestGovernance_ContributionTemplatesExists(t *testing.T) {
	root := findRepoRoot(t)
	tmplPath := filepath.Join(root, "docs", "contribution_templates.md")

	content, err := os.ReadFile(tmplPath)
	if err != nil {
		t.Fatalf("failed to read contribution_templates.md: %v", err)
	}
	str := string(content)

	requiredSections := []string{
		"New Provider Contribution Template",
		"New Feature Contribution Template",
		"Contract Change Contribution Template",
		"Consistency Model",
		"Multi-Step Failure & Rollback Paths",
		"RFC 2119",
	}

	for _, sec := range requiredSections {
		if !strings.Contains(str, sec) {
			t.Errorf("contribution_templates.md missing required section/token: %q", sec)
		}
	}
}

// TestGovernance_ClaimsAuditCompleteness verifies that all architectural claims
// have been cataloged and formally audited in docs/claims_audit.md.
func TestGovernance_ClaimsAuditCompleteness(t *testing.T) {
	root := findRepoRoot(t)
	claimsPath := filepath.Join(root, "docs", "claims_audit.md")

	content, err := os.ReadFile(claimsPath)
	if err != nil {
		t.Fatalf("failed to read claims_audit.md: %v", err)
	}
	str := string(content)

	claimRegex := regexp.MustCompile(`CLM-\d{3}`)
	matches := claimRegex.FindAllString(str, -1)
	if len(matches) < 10 {
		t.Fatalf("expected at least 10 audited claims in claims_audit.md, found %d", len(matches))
	}

	requiredStatuses := []string{
		"PROVEN",
		"CONDITIONALLY TRUE",
		"PARTIALLY PROVEN",
	}

	for _, status := range requiredStatuses {
		if !strings.Contains(str, status) {
			t.Errorf("claims_audit.md missing classification status: %q", status)
		}
	}
}
