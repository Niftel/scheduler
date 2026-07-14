package core

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// TestReconcilerHeartbeatAware proves the reconciler is heartbeat-aware and, for
// LOCAL runs, recovery-aware (#45):
//   - a job running for an hour but heartbeating recently is left alone;
//   - a stale/never-heartbeated LOCAL run is parked in 'reconciling' (its job left
//     running) to give the executor a window to resume it — not immediately lost;
//   - a parked LOCAL run whose recovery deadline has passed is then declared lost
//     (job -> error).
//
// Requires TEST_DATABASE_URL (migrated); skips otherwise.
func TestReconcilerHeartbeatAware(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping reconciler integration test")
	}
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		t.Skipf("cannot reach TEST_DATABASE_URL: %v", err)
	}
	defer db.Close()

	sched := NewScheduler(db, time.Second, nil)

	// newRun inserts a 'running' job + run with the given ages. hbAgo < 0 means
	// "never heartbeated" (NULL last_heartbeat_at).
	newRun := func(startedAgo, hbAgo time.Duration) (int64, uuid.UUID) {
		var jobID int64
		if err := db.QueryRow(`INSERT INTO unified_jobs (name, status) VALUES ('recon-test', 'running') RETURNING id`).Scan(&jobID); err != nil {
			t.Fatalf("insert job: %v", err)
		}
		runID := uuid.New()
		if _, err := db.Exec(`INSERT INTO execution_runs (id, unified_job_id, state) VALUES ($1, $2, 'running')`, runID, jobID); err != nil {
			t.Fatalf("insert run: %v", err)
		}
		if _, err := db.Exec(`UPDATE execution_runs SET started_at = now() - make_interval(secs => $1) WHERE id = $2`, startedAgo.Seconds(), runID); err != nil {
			t.Fatalf("set started_at: %v", err)
		}
		if hbAgo >= 0 {
			if _, err := db.Exec(`UPDATE execution_runs SET last_heartbeat_at = now() - make_interval(secs => $1) WHERE id = $2`, hbAgo.Seconds(), runID); err != nil {
				t.Fatalf("set heartbeat: %v", err)
			}
		}
		t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM unified_jobs WHERE id = $1`, jobID) })
		return jobID, runID
	}

	aliveJob, aliveRun := newRun(time.Hour, 10*time.Second) // long-running but healthy
	staleJob, staleRun := newRun(time.Hour, 10*time.Minute) // heartbeat went stale
	neverJob, neverRun := newRun(10*time.Minute, -1)        // started, never heartbeated

	// A LOCAL run (no runner host) already parked in 'reconciling' whose recovery
	// deadline has passed: the executor never resumed it, so it is now truly lost.
	lostJob, lostRun := newRun(time.Hour, 20*time.Minute)
	if _, err := db.Exec(
		`UPDATE execution_runs SET state='reconciling', reconcile_after = now() - make_interval(secs => 60) WHERE id = $1`,
		lostRun); err != nil {
		t.Fatalf("park run in reconciling: %v", err)
	}

	if err := sched.processTimedOutJobs(context.Background()); err != nil {
		t.Fatalf("processTimedOutJobs: %v", err)
	}

	// Alive run must be untouched — this is the regression the old blanket
	// timeout caused (it would have failed a healthy hour-long job).
	if got := runState(t, db, aliveRun); got != "running" {
		t.Fatalf("alive long-running run was wrongly reconciled to %q", got)
	}
	if got := jobStatus(t, db, aliveJob); got != "running" {
		t.Fatalf("alive job status changed to %q", got)
	}

	// A stale LOCAL run is NOT immediately lost: the executor persists the same WAL
	// and resumes interrupted local runs on startup, so it is parked in
	// 'reconciling' with a recovery deadline and its job is left running (#45).
	if got := runState(t, db, staleRun); got != "reconciling" {
		t.Fatalf("stale local run state = %q, want reconciling (parked for recovery)", got)
	}
	if got := jobStatus(t, db, staleJob); got != "running" {
		t.Fatalf("stale job status = %q, want running (not errored while recoverable)", got)
	}

	// A never-heartbeated LOCAL run (past the start grace) is likewise parked, not
	// immediately lost.
	if got := runState(t, db, neverRun); got != "reconciling" {
		t.Fatalf("never-heartbeated local run state = %q, want reconciling", got)
	}
	if got := jobStatus(t, db, neverJob); got != "running" {
		t.Fatalf("never-heartbeated job status = %q, want running", got)
	}

	// The parked local run past its recovery deadline is now truly lost; job errors.
	if got := runState(t, db, lostRun); got != "lost" {
		t.Fatalf("expired-reconciling local run state = %q, want lost", got)
	}
	if got := jobStatus(t, db, lostJob); got != "error" {
		t.Fatalf("lost job status = %q, want error", got)
	}
}

func runState(t *testing.T, db *sqlx.DB, runID uuid.UUID) string {
	t.Helper()
	var s string
	if err := db.Get(&s, `SELECT state FROM execution_runs WHERE id = $1`, runID); err != nil {
		t.Fatalf("get run state: %v", err)
	}
	return s
}

func jobStatus(t *testing.T, db *sqlx.DB, jobID int64) string {
	t.Helper()
	var s string
	if err := db.Get(&s, `SELECT status FROM unified_jobs WHERE id = $1`, jobID); err != nil {
		t.Fatalf("get job status: %v", err)
	}
	return s
}
