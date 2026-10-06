package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/suhwr/blobkit"
)

// Store implements blobkit.MetadataStore for any standard SQL database.
type Store struct {
	db      *sql.DB
	dialect Dialect
}

// Option configures Store settings.
type Option func(*Store)

// NewStore creates a new SQL-backed MetadataStore.
func NewStore(db *sql.DB, dialect Dialect, opts ...Option) (*Store, error) {
	if db == nil {
		return nil, errors.New("blobkit/sql: database connection cannot be nil")
	}
	s := &Store{
		db:      db,
		dialect: dialect,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// DB returns the underlying sql.DB instance.
func (s *Store) DB() *sql.DB {
	return s.db
}

// Dialect returns the active dialect.
func (s *Store) Dialect() Dialect {
	return s.dialect
}

// Close closes the database connection pool.
func (s *Store) Close() error {
	return s.db.Close()
}

// Save creates or updates an object record in the SQL registry.
func (s *Store) Save(ctx context.Context, record *blobkit.Record) error {
	if record == nil || record.ObjectID == "" {
		return blobkit.ErrInvalidID
	}

	metaJSON, err := json.Marshal(record.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	now := time.Now().UTC()
	createdAt := record.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := now

	var upsertClause string
	switch s.dialect {
	case DialectPostgres:
		upsertClause = `ON CONFLICT (object_id) DO UPDATE SET
			namespace = EXCLUDED.namespace,
			owner_id = EXCLUDED.owner_id,
			storage_key = EXCLUDED.storage_key,
			bucket = EXCLUDED.bucket,
			provider = EXCLUDED.provider,
			mime_type = EXCLUDED.mime_type,
			size = EXCLUDED.size,
			checksum_sha256 = EXCLUDED.checksum_sha256,
			etag = EXCLUDED.etag,
			original_filename = EXCLUDED.original_filename,
			visibility = EXCLUDED.visibility,
			status = EXCLUDED.status,
			metadata = EXCLUDED.metadata,
			retention_until = EXCLUDED.retention_until,
			expires_at = EXCLUDED.expires_at,
			legal_hold = EXCLUDED.legal_hold,
			client_checksum = EXCLUDED.client_checksum,
			updated_at = EXCLUDED.updated_at,
			deleted_at = EXCLUDED.deleted_at`
	default: // SQLite
		upsertClause = `ON CONFLICT (object_id) DO UPDATE SET
			namespace = excluded.namespace,
			owner_id = excluded.owner_id,
			storage_key = excluded.storage_key,
			bucket = excluded.bucket,
			provider = excluded.provider,
			mime_type = excluded.mime_type,
			size = excluded.size,
			checksum_sha256 = excluded.checksum_sha256,
			etag = excluded.etag,
			original_filename = excluded.original_filename,
			visibility = excluded.visibility,
			status = excluded.status,
			metadata = excluded.metadata,
			retention_until = excluded.retention_until,
			expires_at = excluded.expires_at,
			legal_hold = excluded.legal_hold,
			client_checksum = excluded.client_checksum,
			updated_at = excluded.updated_at,
			deleted_at = excluded.deleted_at`
	}

	query := fmt.Sprintf(`INSERT INTO blobkit_records (
		object_id, namespace, owner_id, storage_key, bucket, provider,
		mime_type, size, checksum_sha256, etag, original_filename,
		visibility, status, metadata, retention_until, expires_at,
		legal_hold, client_checksum, created_at, updated_at, deleted_at
	) VALUES (%s) %s;`, s.dialect.Placeholders(1, 21), upsertClause)

	_, err = s.db.ExecContext(ctx, query,
		record.ObjectID,
		record.Namespace,
		record.OwnerID,
		record.Key,
		record.Bucket,
		record.Provider,
		record.MIMEType,
		record.Size,
		record.ChecksumSHA256,
		record.ETag,
		record.OriginalFilename,
		string(record.Visibility),
		string(record.Status),
		string(metaJSON),
		record.RetentionUntil,
		record.ExpiresAt,
		record.LegalHold,
		record.ClientChecksum,
		createdAt,
		updatedAt,
		record.DeletedAt,
	)
	if err != nil {
		return fmt.Errorf("sql save record: %w", err)
	}

	record.CreatedAt = createdAt
	record.UpdatedAt = updatedAt
	return nil
}

// GetByID retrieves an active record by its logical ObjectID.
func (s *Store) GetByID(ctx context.Context, objectID string) (*blobkit.Record, error) {
	if objectID == "" {
		return nil, blobkit.ErrInvalidID
	}

	query := fmt.Sprintf(`SELECT
		object_id, namespace, owner_id, storage_key, bucket, provider,
		mime_type, size, checksum_sha256, etag, original_filename,
		visibility, status, metadata, retention_until, expires_at,
		legal_hold, client_checksum, created_at, updated_at, deleted_at
	FROM blobkit_records WHERE object_id = %s;`, s.dialect.Placeholder(1))

	row := s.db.QueryRowContext(ctx, query, objectID)
	rec, err := s.scanRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, blobkit.ErrObjectNotFound
		}
		return nil, fmt.Errorf("sql get by id: %w", err)
	}

	if rec.Status == blobkit.StateDeleted {
		return nil, blobkit.ErrObjectNotFound
	}

	return rec, nil
}

// GetByKey retrieves an active record by its physical storage key.
func (s *Store) GetByKey(ctx context.Context, key string) (*blobkit.Record, error) {
	if key == "" {
		return nil, blobkit.ErrInvalidKey
	}

	query := fmt.Sprintf(`SELECT
		object_id, namespace, owner_id, storage_key, bucket, provider,
		mime_type, size, checksum_sha256, etag, original_filename,
		visibility, status, metadata, retention_until, expires_at,
		legal_hold, client_checksum, created_at, updated_at, deleted_at
	FROM blobkit_records WHERE storage_key = %s;`, s.dialect.Placeholder(1))

	row := s.db.QueryRowContext(ctx, query, key)
	rec, err := s.scanRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, blobkit.ErrObjectNotFound
		}
		return nil, fmt.Errorf("sql get by key: %w", err)
	}

	if rec.Status == blobkit.StateDeleted {
		return nil, blobkit.ErrObjectNotFound
	}

	return rec, nil
}

// Find queries records matching filter criteria.
func (s *Store) Find(ctx context.Context, filter blobkit.Filter) ([]blobkit.Record, error) {
	var whereClauses []string
	var args []any
	argIdx := 1

	if filter.ObjectID != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("object_id = %s", s.dialect.Placeholder(argIdx)))
		args = append(args, filter.ObjectID)
		argIdx++
	}
	if filter.Key != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("storage_key = %s", s.dialect.Placeholder(argIdx)))
		args = append(args, filter.Key)
		argIdx++
	}
	if filter.Namespace != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("namespace = %s", s.dialect.Placeholder(argIdx)))
		args = append(args, filter.Namespace)
		argIdx++
	}
	if filter.OwnerID != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("owner_id = %s", s.dialect.Placeholder(argIdx)))
		args = append(args, filter.OwnerID)
		argIdx++
	}
	if filter.MIMEType != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("mime_type = %s", s.dialect.Placeholder(argIdx)))
		args = append(args, filter.MIMEType)
		argIdx++
	}
	if filter.OriginalFilename != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("original_filename = %s", s.dialect.Placeholder(argIdx)))
		args = append(args, filter.OriginalFilename)
		argIdx++
	}
	if filter.Status != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("status = %s", s.dialect.Placeholder(argIdx)))
		args = append(args, string(filter.Status))
		argIdx++
	}
	if filter.Provider != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("provider = %s", s.dialect.Placeholder(argIdx)))
		args = append(args, filter.Provider)
		argIdx++
	}
	if filter.CreatedAfter != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("created_at >= %s", s.dialect.Placeholder(argIdx)))
		args = append(args, *filter.CreatedAfter)
		argIdx++
	}
	if filter.CreatedBefore != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("created_at <= %s", s.dialect.Placeholder(argIdx)))
		args = append(args, *filter.CreatedBefore)
		argIdx++
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	query := fmt.Sprintf(`SELECT
		object_id, namespace, owner_id, storage_key, bucket, provider,
		mime_type, size, checksum_sha256, etag, original_filename,
		visibility, status, metadata, retention_until, expires_at,
		legal_hold, client_checksum, created_at, updated_at, deleted_at
	FROM blobkit_records
	%s
	ORDER BY created_at ASC
	LIMIT %s OFFSET %s;`, whereSQL, s.dialect.Placeholder(argIdx), s.dialect.Placeholder(argIdx+1))

	args = append(args, limit, filter.Offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sql find query: %w", err)
	}
	defer rows.Close()

	var results []blobkit.Record
	for rows.Next() {
		rec, err := s.scanRecord(rows)
		if err != nil {
			return nil, fmt.Errorf("sql scan record: %w", err)
		}

		// Filter user-defined metadata in Go for universal dialect parity
		if len(filter.Metadata) > 0 {
			match := true
			for k, v := range filter.Metadata {
				if rec.Metadata == nil || rec.Metadata[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}

		results = append(results, *rec)
	}

	return results, rows.Err()
}

// UpdateStatus transitions an object lifecycle state.
func (s *Store) UpdateStatus(ctx context.Context, objectID string, status blobkit.LifecycleState) error {
	if objectID == "" {
		return blobkit.ErrInvalidID
	}

	now := time.Now().UTC()
	var query string
	var err error

	if status == blobkit.StateDeleted {
		query = fmt.Sprintf(`UPDATE blobkit_records SET status = %s, updated_at = %s, deleted_at = %s WHERE object_id = %s;`,
			s.dialect.Placeholder(1), s.dialect.Placeholder(2), s.dialect.Placeholder(3), s.dialect.Placeholder(4))
		_, err = s.db.ExecContext(ctx, query, string(status), now, now, objectID)
	} else {
		query = fmt.Sprintf(`UPDATE blobkit_records SET status = %s, updated_at = %s WHERE object_id = %s;`,
			s.dialect.Placeholder(1), s.dialect.Placeholder(2), s.dialect.Placeholder(3))
		_, err = s.db.ExecContext(ctx, query, string(status), now, objectID)
	}

	if err != nil {
		return fmt.Errorf("sql update status: %w", err)
	}
	return nil
}

// UpdateMetadata replaces user-defined metadata attributes for an object.
func (s *Store) UpdateMetadata(ctx context.Context, objectID string, metadata map[string]string) error {
	if objectID == "" {
		return blobkit.ErrInvalidID
	}

	metaJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	now := time.Now().UTC()
	query := fmt.Sprintf(`UPDATE blobkit_records SET metadata = %s, updated_at = %s WHERE object_id = %s;`,
		s.dialect.Placeholder(1), s.dialect.Placeholder(2), s.dialect.Placeholder(3))

	res, err := s.db.ExecContext(ctx, query, string(metaJSON), now, objectID)
	if err != nil {
		return fmt.Errorf("sql update metadata: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return blobkit.ErrObjectNotFound
	}
	return nil
}

// UpdateFilename updates client-facing presentation filename.
func (s *Store) UpdateFilename(ctx context.Context, objectID string, newFilename string) error {
	if objectID == "" {
		return blobkit.ErrInvalidID
	}

	now := time.Now().UTC()
	query := fmt.Sprintf(`UPDATE blobkit_records SET original_filename = %s, updated_at = %s WHERE object_id = %s;`,
		s.dialect.Placeholder(1), s.dialect.Placeholder(2), s.dialect.Placeholder(3))

	res, err := s.db.ExecContext(ctx, query, newFilename, now, objectID)
	if err != nil {
		return fmt.Errorf("sql update filename: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return blobkit.ErrObjectNotFound
	}
	return nil
}

// Delete marks an object record as soft-deleted.
func (s *Store) Delete(ctx context.Context, objectID string) error {
	return s.UpdateStatus(ctx, objectID, blobkit.StateDeleted)
}

// HardDelete permanently purges the object record from the database.
func (s *Store) HardDelete(ctx context.Context, objectID string) error {
	if objectID == "" {
		return blobkit.ErrInvalidID
	}

	query := fmt.Sprintf(`DELETE FROM blobkit_records WHERE object_id = %s;`, s.dialect.Placeholder(1))
	_, err := s.db.ExecContext(ctx, query, objectID)
	if err != nil {
		return fmt.Errorf("sql hard delete: %w", err)
	}
	return nil
}

// FindExpired returns committed records whose scheduled ExpiresAt is on or before cutoff.
func (s *Store) FindExpired(ctx context.Context, before time.Time, limit int) ([]blobkit.Record, error) {
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`SELECT
		object_id, namespace, owner_id, storage_key, bucket, provider,
		mime_type, size, checksum_sha256, etag, original_filename,
		visibility, status, metadata, retention_until, expires_at,
		legal_hold, client_checksum, created_at, updated_at, deleted_at
	FROM blobkit_records
	WHERE status = %s AND expires_at IS NOT NULL AND expires_at <= %s
	ORDER BY expires_at ASC
	LIMIT %s;`, s.dialect.Placeholder(1), s.dialect.Placeholder(2), s.dialect.Placeholder(3))

	rows, err := s.db.QueryContext(ctx, query, string(blobkit.StateCommitted), before, limit)
	if err != nil {
		return nil, fmt.Errorf("sql find expired: %w", err)
	}
	defer rows.Close()

	var results []blobkit.Record
	for rows.Next() {
		rec, err := s.scanRecord(rows)
		if err != nil {
			return nil, fmt.Errorf("sql scan expired record: %w", err)
		}
		results = append(results, *rec)
	}

	return results, rows.Err()
}

// SaveSession persists or updates an upload session in the database.
func (s *Store) SaveSession(ctx context.Context, session *blobkit.UploadSession) error {
	if session == nil || session.ID == "" {
		return blobkit.ErrInvalidID
	}

	partsJSON, err := json.Marshal(session.Parts)
	if err != nil {
		return fmt.Errorf("marshal session parts: %w", err)
	}

	now := time.Now().UTC()
	createdAt := session.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}

	var upsertClause string
	switch s.dialect {
	case DialectPostgres:
		upsertClause = `ON CONFLICT (session_id) DO UPDATE SET
			object_id = EXCLUDED.object_id,
			storage_key = EXCLUDED.storage_key,
			bucket = EXCLUDED.bucket,
			provider = EXCLUDED.provider,
			upload_id = EXCLUDED.upload_id,
			part_size = EXCLUDED.part_size,
			total_size = EXCLUDED.total_size,
			status = EXCLUDED.status,
			parts = EXCLUDED.parts,
			expires_at = EXCLUDED.expires_at`
	default:
		upsertClause = `ON CONFLICT (session_id) DO UPDATE SET
			object_id = excluded.object_id,
			storage_key = excluded.storage_key,
			bucket = excluded.bucket,
			provider = excluded.provider,
			upload_id = excluded.upload_id,
			part_size = excluded.part_size,
			total_size = excluded.total_size,
			status = excluded.status,
			parts = excluded.parts,
			expires_at = excluded.expires_at`
	}

	query := fmt.Sprintf(`INSERT INTO blobkit_sessions (
		session_id, object_id, storage_key, bucket, provider, upload_id,
		part_size, total_size, status, parts, expires_at, created_at
	) VALUES (%s) %s;`, s.dialect.Placeholders(1, 12), upsertClause)

	_, err = s.db.ExecContext(ctx, query,
		session.ID,
		session.ObjectID,
		session.Key,
		session.Bucket,
		session.Provider,
		session.UploadID,
		session.PartSize,
		session.TotalSize,
		string(session.Status),
		string(partsJSON),
		session.ExpiresAt,
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("sql save session: %w", err)
	}

	session.CreatedAt = createdAt
	return nil
}

// GetSession retrieves an active upload session by ID.
func (s *Store) GetSession(ctx context.Context, sessionID string) (*blobkit.UploadSession, error) {
	if sessionID == "" {
		return nil, blobkit.ErrInvalidID
	}

	query := fmt.Sprintf(`SELECT
		session_id, object_id, storage_key, bucket, provider, upload_id,
		part_size, total_size, status, parts, expires_at, created_at
	FROM blobkit_sessions WHERE session_id = %s;`, s.dialect.Placeholder(1))

	row := s.db.QueryRowContext(ctx, query, sessionID)

	var (
		sess      blobkit.UploadSession
		partsJSON string
		status    string
	)

	err := row.Scan(
		&sess.ID,
		&sess.ObjectID,
		&sess.Key,
		&sess.Bucket,
		&sess.Provider,
		&sess.UploadID,
		&sess.PartSize,
		&sess.TotalSize,
		&status,
		&partsJSON,
		&sess.ExpiresAt,
		&sess.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, blobkit.ErrSessionNotFound
		}
		return nil, fmt.Errorf("sql get session: %w", err)
	}

	sess.Status = blobkit.SessionState(status)
	if sess.Status == blobkit.SessionAborted {
		return nil, blobkit.ErrSessionNotFound
	}

	if partsJSON != "" {
		_ = json.Unmarshal([]byte(partsJSON), &sess.Parts)
	}

	return &sess, nil
}

// DeleteSession removes an upload session.
func (s *Store) DeleteSession(ctx context.Context, sessionID string) error {
	if sessionID == "" {
		return blobkit.ErrInvalidID
	}

	query := fmt.Sprintf(`DELETE FROM blobkit_sessions WHERE session_id = %s;`, s.dialect.Placeholder(1))
	_, err := s.db.ExecContext(ctx, query, sessionID)
	if err != nil {
		return fmt.Errorf("sql delete session: %w", err)
	}
	return nil
}

// FindStaleSessions returns active sessions whose ExpiresAt is on or before cutoff.
func (s *Store) FindStaleSessions(ctx context.Context, before time.Time, limit int) ([]blobkit.UploadSession, error) {
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`SELECT
		session_id, object_id, storage_key, bucket, provider, upload_id,
		part_size, total_size, status, parts, expires_at, created_at
	FROM blobkit_sessions
	WHERE status = %s AND expires_at <= %s
	ORDER BY expires_at ASC
	LIMIT %s;`, s.dialect.Placeholder(1), s.dialect.Placeholder(2), s.dialect.Placeholder(3))

	rows, err := s.db.QueryContext(ctx, query, string(blobkit.SessionActive), before, limit)
	if err != nil {
		return nil, fmt.Errorf("sql find stale sessions: %w", err)
	}
	defer rows.Close()

	var results []blobkit.UploadSession
	for rows.Next() {
		var (
			sess      blobkit.UploadSession
			partsJSON string
			status    string
		)
		err := rows.Scan(
			&sess.ID,
			&sess.ObjectID,
			&sess.Key,
			&sess.Bucket,
			&sess.Provider,
			&sess.UploadID,
			&sess.PartSize,
			&sess.TotalSize,
			&status,
			&partsJSON,
			&sess.ExpiresAt,
			&sess.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("sql scan stale session: %w", err)
		}
		sess.Status = blobkit.SessionState(status)
		if partsJSON != "" {
			_ = json.Unmarshal([]byte(partsJSON), &sess.Parts)
		}
		results = append(results, sess)
	}

	return results, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func (s *Store) scanRecord(scanner rowScanner) (*blobkit.Record, error) {
	var (
		rec        blobkit.Record
		visibility string
		status     string
		metaRaw    string
		retention  sql.NullTime
		expires    sql.NullTime
		deleted    sql.NullTime
	)

	err := scanner.Scan(
		&rec.ObjectID,
		&rec.Namespace,
		&rec.OwnerID,
		&rec.Key,
		&rec.Bucket,
		&rec.Provider,
		&rec.MIMEType,
		&rec.Size,
		&rec.ChecksumSHA256,
		&rec.ETag,
		&rec.OriginalFilename,
		&visibility,
		&status,
		&metaRaw,
		&retention,
		&expires,
		&rec.LegalHold,
		&rec.ClientChecksum,
		&rec.CreatedAt,
		&rec.UpdatedAt,
		&deleted,
	)
	if err != nil {
		return nil, err
	}

	rec.Visibility = blobkit.Visibility(visibility)
	rec.Status = blobkit.LifecycleState(status)

	if retention.Valid {
		rec.RetentionUntil = &retention.Time
	}
	if expires.Valid {
		rec.ExpiresAt = &expires.Time
	}
	if deleted.Valid {
		rec.DeletedAt = &deleted.Time
	}

	if metaRaw != "" && metaRaw != "{}" {
		var meta map[string]string
		if jsonErr := json.Unmarshal([]byte(metaRaw), &meta); jsonErr == nil {
			rec.Metadata = meta
		}
	}

	return &rec, nil
}
