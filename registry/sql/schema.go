package sql

import (
	"context"
	"database/sql"
	"fmt"
)

// AutoMigrate applies schema definitions and indexing for the chosen SQL dialect.
func AutoMigrate(ctx context.Context, db *sql.DB, dialect Dialect) error {
	var queries []string

	switch dialect {
	case DialectPostgres:
		queries = []string{
			`CREATE TABLE IF NOT EXISTS blobkit_records (
				object_id VARCHAR(64) PRIMARY KEY,
				namespace VARCHAR(255) NOT NULL,
				owner_id VARCHAR(255) NOT NULL DEFAULT '',
				storage_key VARCHAR(1024) NOT NULL UNIQUE,
				bucket VARCHAR(255) NOT NULL,
				provider VARCHAR(255) NOT NULL,
				mime_type VARCHAR(255) NOT NULL,
				size BIGINT NOT NULL,
				checksum_sha256 VARCHAR(64) NOT NULL DEFAULT '',
				etag VARCHAR(255) NOT NULL DEFAULT '',
				original_filename VARCHAR(1024) NOT NULL DEFAULT '',
				visibility VARCHAR(32) NOT NULL,
				status VARCHAR(32) NOT NULL,
				metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
				retention_until TIMESTAMP WITH TIME ZONE,
				expires_at TIMESTAMP WITH TIME ZONE,
				legal_hold BOOLEAN NOT NULL DEFAULT FALSE,
				client_checksum VARCHAR(64) NOT NULL DEFAULT '',
				created_at TIMESTAMP WITH TIME ZONE NOT NULL,
				updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
				deleted_at TIMESTAMP WITH TIME ZONE
			);`,
			`CREATE INDEX IF NOT EXISTS idx_blobkit_records_status_expires ON blobkit_records(status, expires_at);`,
			`CREATE INDEX IF NOT EXISTS idx_blobkit_records_namespace_status ON blobkit_records(namespace, status);`,
			`CREATE INDEX IF NOT EXISTS idx_blobkit_records_owner_status ON blobkit_records(owner_id, status);`,
			`CREATE INDEX IF NOT EXISTS idx_blobkit_records_storage_key ON blobkit_records(storage_key);`,
			`CREATE INDEX IF NOT EXISTS idx_blobkit_records_deleted_at ON blobkit_records(deleted_at);`,

			`CREATE TABLE IF NOT EXISTS blobkit_sessions (
				session_id VARCHAR(64) PRIMARY KEY,
				object_id VARCHAR(64) NOT NULL DEFAULT '',
				storage_key VARCHAR(1024) NOT NULL,
				bucket VARCHAR(255) NOT NULL DEFAULT '',
				provider VARCHAR(255) NOT NULL,
				upload_id VARCHAR(255) NOT NULL,
				part_size BIGINT NOT NULL,
				total_size BIGINT NOT NULL DEFAULT 0,
				status VARCHAR(32) NOT NULL,
				parts JSONB NOT NULL DEFAULT '[]'::jsonb,
				expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL
			);`,
			`CREATE INDEX IF NOT EXISTS idx_blobkit_sessions_status_expires ON blobkit_sessions(status, expires_at);`,
		}

	case DialectSQLite:
		queries = []string{
			`CREATE TABLE IF NOT EXISTS blobkit_records (
				object_id TEXT PRIMARY KEY,
				namespace TEXT NOT NULL,
				owner_id TEXT NOT NULL DEFAULT '',
				storage_key TEXT NOT NULL UNIQUE,
				bucket TEXT NOT NULL,
				provider TEXT NOT NULL,
				mime_type TEXT NOT NULL,
				size INTEGER NOT NULL,
				checksum_sha256 TEXT NOT NULL DEFAULT '',
				etag TEXT NOT NULL DEFAULT '',
				original_filename TEXT NOT NULL DEFAULT '',
				visibility TEXT NOT NULL,
				status TEXT NOT NULL,
				metadata TEXT NOT NULL DEFAULT '{}',
				retention_until DATETIME,
				expires_at DATETIME,
				legal_hold INTEGER NOT NULL DEFAULT 0,
				client_checksum TEXT NOT NULL DEFAULT '',
				created_at DATETIME NOT NULL,
				updated_at DATETIME NOT NULL,
				deleted_at DATETIME
			);`,
			`CREATE INDEX IF NOT EXISTS idx_blobkit_records_status_expires ON blobkit_records(status, expires_at);`,
			`CREATE INDEX IF NOT EXISTS idx_blobkit_records_namespace_status ON blobkit_records(namespace, status);`,
			`CREATE INDEX IF NOT EXISTS idx_blobkit_records_owner_status ON blobkit_records(owner_id, status);`,
			`CREATE INDEX IF NOT EXISTS idx_blobkit_records_storage_key ON blobkit_records(storage_key);`,
			`CREATE INDEX IF NOT EXISTS idx_blobkit_records_deleted_at ON blobkit_records(deleted_at);`,

			`CREATE TABLE IF NOT EXISTS blobkit_sessions (
				session_id TEXT PRIMARY KEY,
				object_id TEXT NOT NULL DEFAULT '',
				storage_key TEXT NOT NULL,
				bucket TEXT NOT NULL DEFAULT '',
				provider TEXT NOT NULL,
				upload_id TEXT NOT NULL,
				part_size INTEGER NOT NULL,
				total_size INTEGER NOT NULL DEFAULT 0,
				status TEXT NOT NULL,
				parts TEXT NOT NULL DEFAULT '[]',
				expires_at DATETIME NOT NULL,
				created_at DATETIME NOT NULL
			);`,
			`CREATE INDEX IF NOT EXISTS idx_blobkit_sessions_status_expires ON blobkit_sessions(status, expires_at);`,
		}

	default:
		return fmt.Errorf("unsupported dialect for automigration: %s", dialect)
	}

	for _, q := range queries {
		if _, err := db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("failed executing migration statement (%s): %w", q, err)
		}
	}

	return nil
}
