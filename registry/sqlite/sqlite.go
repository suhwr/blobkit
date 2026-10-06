package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	regsql "github.com/suhwr/blobkit/registry/sql"
)

// Config configures SQLite connection settings and pragmas.
type Config struct {
	// FilePath is the path to the SQLite file, or ":memory:" for an ephemeral in-memory database.
	FilePath string

	// BusyTimeout controls how long SQLite waits when encountering a locked database before failing.
	// Default is 5 seconds.
	BusyTimeout time.Duration

	// WAL enables Write-Ahead Logging for improved concurrency. Default is true for file-based paths.
	WAL bool

	// AutoMigrate automatically creates required tables and indexes upon initialization. Default is true.
	AutoMigrate bool
}

// New creates and initializes a SQLite-backed MetadataStore.
func New(cfg Config) (*regsql.Store, error) {
	if cfg.FilePath == "" {
		cfg.FilePath = ":memory:"
	}
	if cfg.BusyTimeout <= 0 {
		cfg.BusyTimeout = 5 * time.Second
	}

	dsnParams := []string{
		fmt.Sprintf("_busy_timeout=%d", cfg.BusyTimeout.Milliseconds()),
		"_foreign_keys=1",
	}

	isMemory := cfg.FilePath == ":memory:" || strings.Contains(cfg.FilePath, "mode=memory")
	if !isMemory {
		dsnParams = append(dsnParams, "_journal_mode=WAL")
	}

	dsn := cfg.FilePath
	if len(dsnParams) > 0 {
		separator := "?"
		if strings.Contains(dsn, "?") {
			separator = "&"
		}
		dsn = dsn + separator + strings.Join(dsnParams, "&")
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite open (%s): %w", cfg.FilePath, err)
	}

	// For in-memory SQLite, keep a single open connection so data persists across queries
	if isMemory {
		db.SetMaxOpenConns(1)
	} else {
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
	}

	// Auto-migrate schema
	autoMigrate := true
	if !cfg.AutoMigrate && cfg.FilePath != "" && !isMemory {
		autoMigrate = false
	}
	if autoMigrate {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := regsql.AutoMigrate(ctx, db, regsql.DialectSQLite); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("sqlite automigrate: %w", err)
		}
	}

	return regsql.NewStore(db, regsql.DialectSQLite)
}

// NewFromDB creates a Store from an existing sql.DB instance.
func NewFromDB(db *sql.DB, autoMigrate bool) (*regsql.Store, error) {
	if db == nil {
		return nil, fmt.Errorf("sqlite: db cannot be nil")
	}
	if autoMigrate {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := regsql.AutoMigrate(ctx, db, regsql.DialectSQLite); err != nil {
			return nil, fmt.Errorf("sqlite automigrate: %w", err)
		}
	}
	return regsql.NewStore(db, regsql.DialectSQLite)
}
