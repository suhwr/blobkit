package postgres_test

import (
	"testing"
	"time"

	"github.com/suhwr/blobkit/registry/postgres"
	regsql "github.com/suhwr/blobkit/registry/sql"
)

func TestPostgresConfigValidation(t *testing.T) {
	_, err := postgres.New(postgres.Config{
		DSN: "",
	})
	if err == nil {
		t.Fatalf("expected error for empty DSN")
	}

	_, err = postgres.NewFromDB(nil, false)
	if err == nil {
		t.Fatalf("expected error for nil db")
	}
}

func TestPostgresDialect(t *testing.T) {
	d := regsql.DialectPostgres

	if d.Placeholder(1) != "$1" || d.Placeholder(4) != "$4" {
		t.Fatalf("unexpected postgres placeholder")
	}
	if d.Placeholders(1, 3) != "$1, $2, $3" {
		t.Fatalf("unexpected placeholders list: %s", d.Placeholders(1, 3))
	}
	if d.JSONType() != "JSONB" {
		t.Fatalf("expected JSONB, got %s", d.JSONType())
	}
	if d.TimestampType() != "TIMESTAMP WITH TIME ZONE" {
		t.Fatalf("expected TIMESTAMP WITH TIME ZONE, got %s", d.TimestampType())
	}
	if d.BooleanType() != "BOOLEAN" {
		t.Fatalf("expected BOOLEAN, got %s", d.BooleanType())
	}
}

func TestPostgresConfigDefaults(t *testing.T) {
	cfg := postgres.Config{
		DSN: "postgres://fake:fake@127.0.0.1:5432/test?sslmode=disable",
	}
	if cfg.MaxOpenConns != 0 {
		t.Fatalf("expected 0 before init")
	}
	// Verify timeout can be set
	cfg.ConnMaxLifetime = 10 * time.Minute
	if cfg.ConnMaxLifetime != 10*time.Minute {
		t.Fatalf("mismatched lifetime")
	}
}
