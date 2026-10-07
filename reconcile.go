package blobkit

import (
	"context"
	"errors"
	"strings"
)

// ReconciliationReport records orphan objects, ghost records, and repaired inconsistencies.
type ReconciliationReport struct {
	TotalChecked   int64    `json:"total_checked"`
	OrphanObjects  []string `json:"orphan_objects"`  // present in storage but missing in registry
	GhostRecords   []string `json:"ghost_records"`   // present in registry (StateCommitted) but missing in storage
	MismatchedSize []string `json:"mismatched_size"` // length mismatch between registry and storage
	Repaired       int64    `json:"repaired"`        // records or objects repaired (when dryRun is false)
}

// Reconcile audits and synchronizes database registry state with physical storage backends.
// It detects orphan files (present in driver but absent from registry) and ghost records
// (marked committed in registry but missing in physical storage).
// If dryRun is false, ghost records are marked as StateDeleted.
func (c *Client) Reconcile(ctx context.Context, dryRun bool) (*ReconciliationReport, error) {
	if c.registry == nil {
		return nil, WrapError("reconcile", "", "", errors.New("registry required for reconciliation"))
	}

	report := &ReconciliationReport{
		OrphanObjects:  make([]string, 0),
		GhostRecords:   make([]string, 0),
		MismatchedSize: make([]string, 0),
	}

	// 1. Audit all committed registry records
	recs, err := c.registry.Find(ctx, Filter{Status: StateCommitted})
	if err != nil {
		return nil, WrapError("reconcile_find_records", "", "", err)
	}

	for _, rec := range recs {
		select {
		case <-ctx.Done():
			return report, ctx.Err()
		default:
		}

		report.TotalChecked++

		driver, rErr := c.router.Select(ctx, RouteContext{
			Op:             OpHead,
			Key:            rec.Key,
			ForcedProvider: rec.Provider,
		})
		if rErr != nil {
			report.GhostRecords = append(report.GhostRecords, rec.ObjectID)
			continue
		}

		headObj, headErr := driver.Head(ctx, rec.Key)
		if headErr != nil {
			if IsNotFound(headErr) {
				report.GhostRecords = append(report.GhostRecords, rec.ObjectID)
				if !dryRun {
					_ = c.registry.UpdateStatus(ctx, rec.ObjectID, StateDeleted)
					report.Repaired++
				}
			}
			continue
		}

		if rec.Size > 0 && headObj.Size != rec.Size {
			report.MismatchedSize = append(report.MismatchedSize, rec.Key)
		}
	}

	// 2. Audit storage drivers for orphan files
	drivers := c.router.AllDrivers()
	for _, d := range drivers {
		select {
		case <-ctx.Done():
			return report, ctx.Err()
		default:
		}

		var cursor string
		for {
			listRes, lErr := d.List(ctx, ListOptions{Cursor: cursor, Limit: 1000})
			if lErr != nil {
				break
			}

			for _, obj := range listRes.Objects {
				report.TotalChecked++
				cleanKey := strings.TrimLeft(obj.Key, "/")
				regRec, regErr := c.registry.GetByKey(ctx, cleanKey)
				if regErr != nil || regRec == nil || regRec.Status != StateCommitted {
					report.OrphanObjects = append(report.OrphanObjects, obj.Key)
				}
			}

			if !listRes.IsTruncated || listRes.NextCursor == "" {
				break
			}
			cursor = listRes.NextCursor
		}
	}

	return report, nil
}
