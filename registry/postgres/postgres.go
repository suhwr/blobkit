package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	regsql "github.com/suhwr/blobkit/registry/sql"
)

// Config configures PostgreSQL connection pooling and options.
type Config struct {
	// DSN is the connection string (e.g. "postgres://user:pass@localhost:5432/dbname?sslmode=disable").
	DSN string

	// MaxOpenConns sets the maximum number of open database connections. Default is 25.
	MaxOpenConns int

	// MaxIdleConns sets the maximum number of idle database connections. Default is 10.
	MaxIdleConns int

	// ConnMaxLifetime sets the maximum amount of time a connection may be reused. Default is 5 minutes.
	ConnMaxLifetime time.Duration

	// AutoMigrate automatically creates required tables and indexes upon initialization. Default is true.
	AutoMigrate bool
}

// New creates and initializes a PostgreSQL-backed MetadataStore.
func New(cfg Config) (*regsql.Store, error) {
	if cfg.DSN == "" {
		return nil, fmt.Errorf("postgres: DSN cannot be empty")
	}
	if cfg.MaxOpenConns <= 0 {
		cfg.MaxOpenConns = 25
	}
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = 10
	}
	if cfg.ConnMaxLifetime <= 0 {
		cfg.ConnMaxLifetime = 5 * time.Minute
	}

	db, err := sql.Open("postgres", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres open: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	if cfg.AutoMigrate {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := regsql.AutoMigrate(ctx, db, regsql.DialectPostgres); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("postgres automigrate: %w", err)
		}
	}

	return regsql.NewStore(db, regsql.DialectPostgres)
}

// NewFromDB creates a Store from an existing sql.DB instance (e.g. pgx/v5/stdlib or lib/pq).
func NewFromDB(db *sql.DB, autoMigrate bool) (*regsql.Store, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres: db cannot be nil")
	}
	if autoMigrate {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := regsql.AutoMigrate(ctx, db, regsql.DialectPostgres); err != nil {
			return nil, fmt.Errorf("postgres automigrate: %w", err)
		}
	}
	return regsql.NewStore(db, regsql.DialectPostgres)
}
