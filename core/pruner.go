package core

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

// pruneInterval throttles the retention pruner so it runs at most once an hour,
// regardless of the (much faster) scheduler tick.
const pruneInterval = time.Hour

// pruneBatch caps how many jobs one pass deletes, so a large backlog is worked
// down over several passes instead of one huge transaction.
const pruneBatch = 500

// maybePrune runs the retention pruner when it's enabled (RetentionDays > 0) and
// at least pruneInterval has elapsed since the last run.
func (s *Scheduler) maybePrune(ctx context.Context) {
	if s.RetentionDays <= 0 {
		return // retention disabled — keep everything
	}
	if !s.lastPrune.IsZero() && time.Since(s.lastPrune) < pruneInterval {
		return
	}
	s.lastPrune = time.Now()
	if err := s.pruneOldJobs(ctx); err != nil {
		logger.Error("retention prune failed", "err", err)
	}
}

// pruneOldJobs deletes terminal jobs finished more than RetentionDays ago. It
// removes their log blobs from the object store first (best-effort; a missing
// blob is fine), then deletes the job rows — execution_runs, job_events,
// job_output_chunks and the outbox cascade from unified_jobs. Active jobs are
// never touched.
func (s *Scheduler) pruneOldJobs(ctx context.Context) error {
	var jobIDs []int64
	if err := s.DB.SelectContext(ctx, &jobIDs, `
		SELECT id FROM unified_jobs
		WHERE status IN ('successful','failed','canceled','error')
		  AND finished_at IS NOT NULL
		  AND finished_at < now() - make_interval(days => $1)
		ORDER BY finished_at
		LIMIT $2`, s.RetentionDays, pruneBatch); err != nil {
		return err
	}
	if len(jobIDs) == 0 {
		return nil
	}

	// 1. Delete the object-store blobs backing these jobs' output chunks, before
	// the rows that index them cascade away.
	if s.Logs != nil {
		var keys []string
		q, args, err := sqlx.In(`
			SELECT storage_key FROM job_output_chunks
			WHERE execution_run_id IN (
				SELECT id FROM execution_runs WHERE unified_job_id IN (?))`, jobIDs)
		if err == nil {
			if err := s.DB.SelectContext(ctx, &keys, s.DB.Rebind(q), args...); err == nil {
				for _, k := range keys {
					if derr := s.Logs.Delete(k); derr != nil {
						logger.Error("retention blob delete failed", "key", k, "err", derr)
					}
				}
			}
		}
	}

	// 2. Delete the job rows; runs/events/chunks/outbox cascade.
	q, args, err := sqlx.In(`DELETE FROM unified_jobs WHERE id IN (?)`, jobIDs)
	if err != nil {
		return err
	}
	res, err := s.DB.ExecContext(ctx, s.DB.Rebind(q), args...)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	logger.Info("retention pruned jobs", "count", n, "older_than_days", s.RetentionDays)
	return nil
}
