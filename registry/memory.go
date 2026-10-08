package registry

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/suhwr/blobkit"
)

// MemoryStore is an in-memory thread-safe implementation of Store, useful for testing
// and environments where an external database is not required.
type MemoryStore struct {
	mu       sync.RWMutex
	records  map[string]Record // object_id -> Record
	byKey    map[string]string // storage key -> object_id
	sessions map[string]UploadSession
}

// NewMemoryStore initializes a MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		records:  make(map[string]Record),
		byKey:    make(map[string]string),
		sessions: make(map[string]UploadSession),
	}
}

func (s *MemoryStore) Save(ctx context.Context, record *Record) error {
	if record == nil || record.ObjectID == "" {
		return blobkit.ErrInvalidID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec := *record
	rec.Metadata = cloneMetadata(record.Metadata)
	now := time.Now().UTC()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = now
	}
	rec.UpdatedAt = now

	if rec.Key != "" {
		if existingID, ok := s.byKey[rec.Key]; ok && existingID != rec.ObjectID {
			if existingRec, found := s.records[existingID]; found && existingRec.Status != blobkit.StateDeleted {
				return blobkit.WrapError("registry_save", rec.Key, "", fmt.Errorf("key collision: key %q already belongs to object %q", rec.Key, existingID))
			}
		}
	}

	if oldRec, ok := s.records[rec.ObjectID]; ok {
		if oldRec.Key != rec.Key && oldRec.Key != "" {
			delete(s.byKey, oldRec.Key)
		}
	}

	s.records[rec.ObjectID] = rec
	if rec.Key != "" {
		s.byKey[rec.Key] = rec.ObjectID
	}
	return nil
}

func (s *MemoryStore) GetByID(ctx context.Context, objectID string) (*Record, error) {
	if objectID == "" {
		return nil, blobkit.ErrInvalidID
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.records[objectID]
	if !ok || rec.Status == blobkit.StateDeleted {
		return nil, blobkit.ErrObjectNotFound
	}
	copyRec := rec
	copyRec.Metadata = cloneMetadata(rec.Metadata)
	return &copyRec, nil
}

func (s *MemoryStore) GetByKey(ctx context.Context, key string) (*Record, error) {
	if key == "" {
		return nil, blobkit.ErrInvalidKey
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	id, ok := s.byKey[key]
	if !ok {
		return nil, blobkit.ErrObjectNotFound
	}

	rec, ok := s.records[id]
	if !ok || rec.Status == blobkit.StateDeleted {
		return nil, blobkit.ErrObjectNotFound
	}
	copyRec := rec
	copyRec.Metadata = cloneMetadata(rec.Metadata)
	return &copyRec, nil
}

func (s *MemoryStore) Find(ctx context.Context, filter Filter) ([]Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var matched []Record
	for _, rec := range s.records {
		if rec.Status == blobkit.StateDeleted && filter.Status != blobkit.StateDeleted {
			continue
		}
		if filter.ObjectID != "" && rec.ObjectID != filter.ObjectID {
			continue
		}
		if filter.Key != "" && rec.Key != filter.Key {
			continue
		}
		if filter.Namespace != "" && rec.Namespace != filter.Namespace && !strings.HasPrefix(rec.Namespace, filter.Namespace+"/") {
			continue
		}
		if filter.OwnerID != "" && rec.OwnerID != filter.OwnerID {
			continue
		}
		if filter.MIMEType != "" && rec.MIMEType != filter.MIMEType {
			continue
		}
		if filter.OriginalFilename != "" && rec.OriginalFilename != filter.OriginalFilename {
			continue
		}
		if filter.Status != "" && rec.Status != filter.Status {
			continue
		}
		if filter.Provider != "" && rec.Provider != filter.Provider {
			continue
		}
		if filter.CreatedAfter != nil && rec.CreatedAt.Before(*filter.CreatedAfter) {
			continue
		}
		if filter.CreatedBefore != nil && rec.CreatedAt.After(*filter.CreatedBefore) {
			continue
		}
		if filter.DeletedBefore != nil && (rec.DeletedAt == nil || rec.DeletedAt.After(*filter.DeletedBefore)) {
			continue
		}

		// Metadata key-value matching
		if len(filter.Metadata) > 0 {
			allMatch := true
			for k, v := range filter.Metadata {
				if rec.Metadata[k] != v {
					allMatch = false
					break
				}
			}
			if !allMatch {
				continue
			}
		}

		recCopy := rec
		recCopy.Metadata = cloneMetadata(rec.Metadata)
		matched = append(matched, recCopy)
	}

	// Sort newest first
	sort.Slice(matched, func(i, j int) bool {
		return matched[i].CreatedAt.After(matched[j].CreatedAt)
	})

	// Pagination
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	if offset >= len(matched) {
		return []Record{}, nil
	}

	limit := filter.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	end := offset + limit
	if end > len(matched) {
		end = len(matched)
	}

	return matched[offset:end], nil
}

func (s *MemoryStore) UpdateStatus(ctx context.Context, objectID string, status blobkit.LifecycleState) error {
	if objectID == "" {
		return blobkit.ErrInvalidID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[objectID]
	if !ok {
		return blobkit.ErrObjectNotFound
	}

	rec.Status = status
	rec.UpdatedAt = time.Now().UTC()
	if status == blobkit.StateDeleted {
		now := time.Now().UTC()
		rec.DeletedAt = &now
	}
	s.records[objectID] = rec
	return nil
}

func (s *MemoryStore) UpdateMetadata(ctx context.Context, objectID string, metadata map[string]string) error {
	if objectID == "" {
		return blobkit.ErrInvalidID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[objectID]
	if !ok || rec.Status == blobkit.StateDeleted {
		return blobkit.ErrObjectNotFound
	}

	newMeta := cloneMetadata(rec.Metadata)
	if newMeta == nil {
		newMeta = make(map[string]string)
	}
	for k, v := range metadata {
		newMeta[k] = v
	}
	rec.Metadata = newMeta
	rec.UpdatedAt = time.Now().UTC()
	s.records[objectID] = rec
	return nil
}

func (s *MemoryStore) UpdateFilename(ctx context.Context, objectID string, newFilename string) error {
	if objectID == "" {
		return blobkit.ErrInvalidID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[objectID]
	if !ok || rec.Status == blobkit.StateDeleted {
		return blobkit.ErrObjectNotFound
	}

	rec.OriginalFilename = newFilename
	rec.UpdatedAt = time.Now().UTC()
	s.records[objectID] = rec
	return nil
}

func (s *MemoryStore) Delete(ctx context.Context, objectID string) error {
	if objectID == "" {
		return blobkit.ErrInvalidID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[objectID]
	if !ok {
		return nil
	}

	now := time.Now().UTC()
	rec.Status = blobkit.StateDeleted
	rec.DeletedAt = &now
	rec.UpdatedAt = now
	s.records[objectID] = rec
	return nil
}

func (s *MemoryStore) HardDelete(ctx context.Context, objectID string) error {
	if objectID == "" {
		return blobkit.ErrInvalidID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[objectID]
	if !ok {
		return blobkit.ErrObjectNotFound
	}
	delete(s.records, objectID)
	if rec.Key != "" {
		delete(s.byKey, rec.Key)
	}
	return nil
}

func (s *MemoryStore) FindExpired(ctx context.Context, before time.Time, limit int) ([]Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	var expired []Record
	for _, rec := range s.records {
		if rec.Status != blobkit.StateCommitted {
			continue
		}
		if rec.LegalHold {
			continue
		}
		if rec.ExpiresAt != nil && !rec.ExpiresAt.After(before) {
			recCopy := rec
			recCopy.Metadata = cloneMetadata(rec.Metadata)
			expired = append(expired, recCopy)
			if len(expired) >= limit {
				break
			}
		}
	}
	return expired, nil
}

func (s *MemoryStore) SaveSession(ctx context.Context, session *UploadSession) error {
	if session == nil || session.ID == "" {
		return blobkit.ErrInvalidID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	sess := *session
	sess.Parts = cloneParts(session.Parts)
	s.sessions[session.ID] = sess
	return nil
}

func (s *MemoryStore) GetSession(ctx context.Context, sessionID string) (*UploadSession, error) {
	if sessionID == "" {
		return nil, blobkit.ErrInvalidID
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	sess, ok := s.sessions[sessionID]
	if !ok || sess.Status == blobkit.SessionAborted {
		return nil, blobkit.ErrSessionNotFound
	}

	copySess := sess
	copySess.Parts = cloneParts(sess.Parts)
	return &copySess, nil
}

func (s *MemoryStore) DeleteSession(ctx context.Context, sessionID string) error {
	if sessionID == "" {
		return blobkit.ErrInvalidID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.sessions[sessionID]; !ok {
		return blobkit.ErrSessionNotFound
	}
	delete(s.sessions, sessionID)
	return nil
}

func (s *MemoryStore) FindStaleSessions(ctx context.Context, before time.Time, limit int) ([]UploadSession, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	var stale []UploadSession
	for _, sess := range s.sessions {
		if sess.Status == blobkit.SessionActive && !sess.ExpiresAt.After(before) {
			staleSess := sess
			staleSess.Parts = cloneParts(sess.Parts)
			stale = append(stale, staleSess)
			if len(stale) >= limit {
				break
			}
		}
	}
	return stale, nil
}

func (s *MemoryStore) Close() error {
	s.mu.Lock()
	s.records = make(map[string]Record)
	s.byKey = make(map[string]string)
	s.sessions = make(map[string]UploadSession)
	s.mu.Unlock()
	return nil
}

func cloneMetadata(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

func cloneParts(parts []blobkit.CompletedPart) []blobkit.CompletedPart {
	if parts == nil {
		return nil
	}
	cp := make([]blobkit.CompletedPart, len(parts))
	copy(cp, parts)
	return cp
}
