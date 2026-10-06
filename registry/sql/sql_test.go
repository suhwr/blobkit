package sql_test

import (
	"context"
	"testing"

	regsql "github.com/suhwr/blobkit/registry/sql"
)

func TestDialects(t *testing.T) {
	sqlite := regsql.DialectSQLite
	if sqlite.Placeholder(1) != "?" || sqlite.Placeholder(5) != "?" {
		t.Fatalf("unexpected sqlite placeholder")
	}
	if sqlite.Placeholders(1, 3) != "?, ?, ?" {
		t.Fatalf("unexpected sqlite placeholders list: %s", sqlite.Placeholders(1, 3))
	}
	if sqlite.Placeholders(1, 0) != "" {
		t.Fatalf("expected empty for count 0")
	}
	if sqlite.JSONType() != "TEXT" {
		t.Fatalf("expected TEXT, got %s", sqlite.JSONType())
	}
	if sqlite.TimestampType() != "DATETIME" {
		t.Fatalf("expected DATETIME, got %s", sqlite.TimestampType())
	}
	if sqlite.BooleanType() != "INTEGER" {
		t.Fatalf("expected INTEGER, got %s", sqlite.BooleanType())
	}
}

func TestStoreValidation(t *testing.T) {
	_, err := regsql.NewStore(nil, regsql.DialectSQLite)
	if err == nil {
		t.Fatalf("expected error for nil db")
	}

	err = regsql.AutoMigrate(context.Background(), nil, "unknown_dialect")
	if err == nil {
		t.Fatalf("expected error for unknown dialect")
	}
}
