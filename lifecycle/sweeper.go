package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/suhwr/blobkit"
)

// DistributedLocker provides mutual exclusion for background maintenance workers.
type DistributedLocker interface {
	// TryLock attempts to acquire the lock. Returns unlock func, acquired flag, and error.
	TryLock(ctx context.Context) (unlock func(), acquired bool, err error)
}

// SweeperConfig tunes the automated lifecycle sweeper worker.
type SweeperConfig struct {
	// Interval is the frequency of periodic background sweeps. Default is 1 hour.
	Interval time.Duration

	// SoftDeleteTTL is the grace period before soft-deleted objects are permanently purged.
	// Default is 30 days. If 0 or negative, soft-deleted objects are not auto-purged.
	SoftDeleteTTL time.Duration

	// MultipartStaleTTL is the maximum age of abandoned multipart sessions before aborting.
	// Default is 24 hours.
	MultipartStaleTTL time.Duration

	// BatchSize bounds the number of records inspected or purged per sweep iteration.
	// Default is 100.
	BatchSize int

	// Locker is an optional distributed locker preventing concurrent sweeper executions
	// across multi-node or multi-pod deployments.
	Locker DistributedLocker
}

// SweepResult provides telemetry and diagnostics about a completed sweep iteration.
type SweepResult struct {
	ExpiredPurged    int           `json:"expired_purged"`
	SoftDeletePurged int           `json:"soft_delete_purged"`
	SessionsAborted  int           `json:"sessions_aborted"`
	Duration         time.Duration `json:"duration"`
	Errors           []error       `json:"errors,omitempty"`
}

// Sweeper manages background lifecycle reconciliation: purging expired objects,
// garbage collecting soft-deleted items exceeding retention, and aborting stale multipart sessions.
type Sweeper struct {
	client  *blobkit.Client
	cfg     SweeperConfig
	stopCh  chan struct{}
	doneCh  chan struct{}
	running atomic.Bool
	mu      sync.Mutex
}

// NewSweeper creates a lifecycle Sweeper attached to a BlobKit client.
func NewSweeper(client *blobkit.Client, cfg SweeperConfig) (*Sweeper, error) {
	if client == nil {
		return nil, errors.New("blobkit: client cannot be nil")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Hour
	}
	if cfg.MultipartStaleTTL <= 0 {
		cfg.MultipartStaleTTL = 24 * time.Hour
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}

	return &Sweeper{
		client: client,
		cfg:    cfg,
	}, nil
}

// RunOnce executes a single reconciliation sweep synchronously.
func (s *Sweeper) RunOnce(ctx context.Context) (*SweepResult, error) {
	start := time.Now()
	res := &SweepResult{}

	// 0. Mutual exclusion via distributed locker (if configured)
	if s.cfg.Locker != nil {
		unlock, acquired, err := s.cfg.Locker.TryLock(ctx)
		if err != nil {
			return res, fmt.Errorf("sweeper distributed lock failed: %w", err)
		}
		if !acquired {
			// Another worker instance holds the lock. Exit gracefully.
			return res, nil
		}
		defer unlock()
	}

	now := time.Now().UTC()

	// 1. Purge expired objects in bulk batches
	expiredRecords, err := s.client.FindExpiredObjects(ctx, now, s.cfg.BatchSize)
	if err != nil {
		res.Errors = append(res.Errors, fmt.Errorf("find expired: %w", err))
	} else if len(expiredRecords) > 0 {
		var candidateIDs []string
		for _, rec := range expiredRecords {
			if rec.LegalHold {
				continue
			}
			candidateIDs = append(candidateIDs, rec.ObjectID)
		}
		if len(candidateIDs) > 0 {
			deleted, delErr := s.client.PermanentDeleteBatch(ctx, candidateIDs)
			res.ExpiredPurged += len(deleted)
			if delErr != nil {
				res.Errors = append(res.Errors, fmt.Errorf("purge expired batch: %w", delErr))
			}
		}
	}

	// 2. Purge soft-deleted objects older than SoftDeleteTTL in bulk batches
	if s.cfg.SoftDeleteTTL > 0 {
		cutoff := now.Add(-s.cfg.SoftDeleteTTL)
		softDeleted, err := s.client.FindSoftDeletedObjects(ctx, cutoff, s.cfg.BatchSize)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("find soft deleted: %w", err))
		} else if len(softDeleted) > 0 {
			var candidateIDs []string
			for _, rec := range softDeleted {
				if rec.LegalHold {
					continue
				}
				if rec.RetentionUntil != nil && rec.RetentionUntil.After(now) {
					continue
				}
				candidateIDs = append(candidateIDs, rec.ObjectID)
			}
			if len(candidateIDs) > 0 {
				deleted, delErr := s.client.PermanentDeleteBatch(ctx, candidateIDs)
				res.SoftDeletePurged += len(deleted)
				if delErr != nil {
					res.Errors = append(res.Errors, fmt.Errorf("purge soft deleted batch: %w", delErr))
				}
			}
		}
	}

	// 3. Abort stale multipart upload sessions
	sessionCutoff := now
	staleSessions, err := s.client.FindStaleSessions(ctx, sessionCutoff, s.cfg.BatchSize)
	if err != nil {
		res.Errors = append(res.Errors, fmt.Errorf("find stale sessions: %w", err))
	} else {
		for _, sess := range staleSessions {
			if abortErr := s.client.AbortResumableUpload(ctx, sess.ID); abortErr != nil {
				res.Errors = append(res.Errors, fmt.Errorf("abort session %s: %w", sess.ID, abortErr))
			} else {
				res.SessionsAborted++
			}
		}
	}

	res.Duration = time.Since(start)
	if len(res.Errors) > 0 {
		return res, errors.Join(res.Errors...)
	}
	return res, nil
}

// Start launches the periodic background sweeper loop in a separate goroutine.
func (s *Sweeper) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running.Swap(true) {
		s.mu.Unlock()
		return errors.New("sweeper is already running")
	}
	s.stopCh = make(chan struct{})
	s.doneCh = make(chan struct{})
	s.mu.Unlock()

	go func() {
		defer func() {
			s.running.Store(false)
			close(s.doneCh)
		}()

		ticker := time.NewTicker(s.cfg.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stopCh:
				return
			case <-ticker.C:
				_, _ = s.RunOnce(ctx)
			}
		}
	}()

	return nil
}

// Stop gracefully signals the background sweeper loop to shut down and waits for completion.
func (s *Sweeper) Stop() {
	s.mu.Lock()
	if !s.running.Load() {
		s.mu.Unlock()
		return
	}
	close(s.stopCh)
	done := s.doneCh
	s.mu.Unlock()

	if done != nil {
		<-done
	}
}
