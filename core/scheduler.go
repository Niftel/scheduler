package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/praetordev/env"
	"github.com/praetordev/events"
	"github.com/praetordev/launch"
	"github.com/praetordev/models"
	"github.com/praetordev/objectstore"
	"github.com/praetordev/plog"
	"github.com/praetordev/store"
	"github.com/teambition/rrule-go"
)

// logger is the scheduler package's structured, component-tagged logger. The
// composition root installs the handler (see pkg/plog); this tags every record
// with component=scheduler across the package's files.
var logger = plog.New("scheduler")

type Scheduler struct {
	DB        *sqlx.DB
	Ticker    *time.Ticker
	Publisher EventPublisher

	// Retention pruning (opt-in): when RetentionDays > 0, terminal jobs finished
	// longer ago than that are deleted — their log blobs removed from Logs, then
	// the job rows (runs/events/chunks/outbox cascade). Logs may be nil (skips
	// blob cleanup). See pruner.go.
	Logs          objectstore.LogStore
	RetentionDays int
	lastPrune     time.Time

	// APIURL is the control-plane base URL embedded in the job manifest so the
	// pushed host-runner knows where to report back. Resolved in main from env;
	// empty is valid (callers fall back to the in-cluster default).
	APIURL string
}

func NewScheduler(db *sqlx.DB, interval time.Duration, publisher EventPublisher) *Scheduler {
	return &Scheduler{
		DB:        db,
		Ticker:    time.NewTicker(interval),
		Publisher: publisher,
	}
}

// tickTask is one pass of the scheduler tick. Splitting the tick into named tasks
// keeps each pass independently testable and gives per-task error visibility
// instead of one monolithic loop body.
type tickTask struct {
	name string
	run  func(ctx context.Context) error
}

// tickTasks returns the ordered passes performed every tick. Order is
// significant: claim → relay → schedules → timeouts → workflows → triggers →
// prune. Passes that log internally (workflows/triggers/prune) return nil.
func (s *Scheduler) tickTasks() []tickTask {
	return []tickTask{
		{"pending_jobs", s.processPendingJobs},
		{"relay_outbox", s.relayOutbox},
		{"schedules", s.processSchedules},
		{"timeouts", s.processTimedOutJobs},
		{"workflows", func(ctx context.Context) error { s.processWorkflows(ctx); return nil }},
		{"event_triggers", func(ctx context.Context) error { s.processEventTriggers(ctx); return nil }},
		{"prune", func(ctx context.Context) error { s.maybePrune(ctx); return nil }},
	}
}

// Start runs the tick loop until ctx is canceled. Cancellation is the only stop
// signal (no separate Done channel); the ticker is stopped on exit.
func (s *Scheduler) Start(ctx context.Context) {
	defer s.Ticker.Stop()
	tasks := s.tickTasks()
	logger.Info("scheduler started")
	for {
		select {
		case <-ctx.Done():
			logger.Info("scheduler stopped")
			return
		case <-s.Ticker.C:
			s.runTick(ctx, tasks)
		}
	}
}

// runTick executes every tick task in order, isolating each task's error so one
// failing pass neither aborts the tick nor is silently swallowed.
func (s *Scheduler) runTick(ctx context.Context, tasks []tickTask) {
	tickStart := time.Now()
	for _, t := range tasks {
		if err := t.run(ctx); err != nil {
			logger.Error("tick task failed", "task", t.name, "err", err)
			TickTaskErrors.WithLabelValues(t.name).Inc()
		}
	}
	TickDuration.Observe(time.Since(tickStart).Seconds())
}

func (s *Scheduler) processPendingJobs(ctx context.Context) error {
	// Transaction for atomic claim-and-schedule
	tx, err := s.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. Fetch pending jobs with SKIP LOCKED
	query := `
		SELECT id, name, unified_job_template_id, status, job_args
		FROM unified_jobs
		WHERE status = 'pending' AND current_run_id IS NULL AND NOT cancel_requested
		FOR UPDATE SKIP LOCKED
		LIMIT 10`

	var jobs []models.UnifiedJob
	if err := tx.SelectContext(ctx, &jobs, query); err != nil {
		return fmt.Errorf("failed to select pending jobs: %w", err)
	}

	if len(jobs) == 0 {
		return nil
	}

	for _, job := range jobs {
		// 3. Create Execution Run
		var runID uuid.UUID
		err := tx.QueryRowContext(ctx, `
			INSERT INTO execution_runs (unified_job_id, state)
			VALUES ($1, 'pending')
			RETURNING id`, job.ID).Scan(&runID)

		if err != nil {
			logger.Error("create run for job failed", "job_id", job.ID, "err", err)
			return err // Rollback
		}

		// 4. Update Job
		_, err = tx.ExecContext(ctx, `
			UPDATE unified_jobs 
			SET status = 'queued', current_run_id = $1 
			WHERE id = $2`, runID, job.ID)

		if err != nil {
			logger.Error("update job failed", "job_id", job.ID, "err", err)
			return err // Rollback
		}

		// Resolve the claimed job into an execution manifest. Inventory-sync jobs
		// (no template) and ordinary template jobs each have their own read-only
		// builder in manifest.go; both return the ids we snapshot on the run below.
		var manifest events.JobManifest
		var runnerHostID, credID int64
		if srcID := launch.ParseArgs(job.JobArgs).InventorySourceID; srcID > 0 {
			m, cred, berr := s.buildSyncManifest(ctx, tx, srcID)
			if berr != nil {
				logger.Error("build inventory-sync manifest failed", "job_id", job.ID, "source_id", srcID, "err", berr)
				logExec(ctx, tx, "UPDATE unified_jobs SET status='failed' WHERE id=$1", job.ID)
				continue
			}
			manifest, credID = m, cred
		} else {
			if job.UnifiedJobTemplateID == nil {
				logger.Warn("job has no template - skipping (template required)", "job_id", job.ID)
				logExec(ctx, tx, "UPDATE unified_jobs SET status = 'failed' WHERE id = $1", job.ID)
				continue
			}
			m, rh, cred, berr := s.buildJobManifest(ctx, tx, job)
			if berr != nil {
				logger.Error("build job manifest failed", "job_id", job.ID, "err", berr)
				logExec(ctx, tx, "UPDATE unified_jobs SET status = 'failed' WHERE id = $1", job.ID)
				continue
			}
			manifest, runnerHostID, credID = m, rh, cred
		}

		// Snapshot the resolved runner host + credential onto the run: the
		// reconciler can then SSH back to the SAME host after an outage, and
		// credential resolution stays run-scoped, even if the template/inventory
		// changes later. A 0 runner-host id means localhost (nothing to snapshot).
		if runnerHostID != 0 {
			if _, err := tx.ExecContext(ctx,
				`UPDATE execution_runs SET runner_host_id = $1 WHERE id = $2`, runnerHostID, runID); err != nil {
				logger.Error("snapshot runner_host_id failed", "job_id", job.ID, "err", err)
			}
		}
		if credID != 0 {
			if _, err := tx.ExecContext(ctx,
				`UPDATE execution_runs SET credential_id = $1 WHERE id = $2`, credID, runID); err != nil {
				logger.Error("snapshot credential id on run failed", "job_id", job.ID, "run_id", runID, "err", err)
			}
		}

		// Enqueue the launch in the transactional outbox rather than publishing
		// inline. The outbox row commits atomically with the run; the relay
		// delivers it — no dual-write hazard (orphaned run / stranded job).
		req := &events.ExecutionRequest{
			ManifestVersion: events.CurrentManifestVersion,
			ExecutionRunID:  runID,
			UnifiedJobID:    job.ID,
			JobManifest:     manifest,
			CreatedAt:       time.Now(),
		}
		payload, err := json.Marshal(req)
		if err != nil {
			logger.Error("marshal execution request failed", "run_id", runID, "err", err)
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO execution_outbox (execution_run_id, payload) VALUES ($1, $2)`,
			runID, payload,
		); err != nil {
			logger.Error("enqueue execution request failed", "run_id", runID, "err", err)
			return err
		}
		logger.Info("enqueued execution request", "job_id", job.ID, "run_id", runID, "playbook", manifest.Playbook)
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	JobsDispatched.Add(float64(len(jobs)))
	return nil
}

// relayOutbox publishes committed-but-unsent launches to the durable request
// stream and marks them sent. FOR UPDATE SKIP LOCKED makes it safe to run from
// multiple schedulers; the request stream's dedup window makes a re-publish
// after a crash (sent on the bus but not yet marked) harmless.
func (s *Scheduler) relayOutbox(ctx context.Context) error {
	// Recover orphaned claims: rows left 'sending' by a relay that crashed after
	// claiming but before publishing/marking. Only stale claims are reset so a
	// concurrent scheduler's in-flight batch isn't disturbed; the request stream's
	// dedup window makes any resulting re-publish harmless.
	if _, err := s.DB.ExecContext(ctx, `
		UPDATE execution_outbox SET status = 'pending'
		WHERE status = 'sending' AND sent_at < now() - interval '2 minutes'`); err != nil {
		logger.Error("outbox: recover stale claims failed", "err", err)
	}

	// Atomically claim a batch. The single UPDATE ... RETURNING takes and releases
	// its row locks within the statement, so the NATS publishes below run with no
	// open transaction and no locks held — a publish can no longer be stranded
	// inside an uncommitted tx (the previous dual-write hazard).
	type outboxRow struct {
		ID      int64           `db:"id"`
		Payload json.RawMessage `db:"payload"`
	}
	var rows []outboxRow
	if err := s.DB.SelectContext(ctx, &rows, `
		UPDATE execution_outbox SET status = 'sending', sent_at = now()
		WHERE id IN (
			SELECT id FROM execution_outbox
			WHERE status = 'pending'
			ORDER BY id
			FOR UPDATE SKIP LOCKED
			LIMIT 50
		)
		RETURNING id, payload`); err != nil {
		return fmt.Errorf("failed to claim outbox rows: %w", err)
	}

	for _, row := range rows {
		var req events.ExecutionRequest
		if err := json.Unmarshal(row.Payload, &req); err != nil {
			logger.Error("outbox: dropping unparseable row", "row_id", row.ID, "err", err)
			logExec(ctx, s.DB, `UPDATE execution_outbox SET status = 'failed', attempts = attempts + 1 WHERE id = $1`, row.ID)
			continue
		}
		if err := s.Publisher.PublishExecutionRequest(&req); err != nil {
			// Return the row to the queue for the next tick.
			logger.Error("outbox: publish failed (will retry)", "row_id", row.ID, "err", err)
			logExec(ctx, s.DB, `UPDATE execution_outbox SET status = 'pending', attempts = attempts + 1 WHERE id = $1`, row.ID)
			continue
		}
		if _, err := s.DB.ExecContext(ctx,
			`UPDATE execution_outbox SET status = 'sent', sent_at = now(), attempts = attempts + 1 WHERE id = $1`,
			row.ID); err != nil {
			// Published but couldn't mark sent; leaving it 'sending' means stale-claim
			// recovery requeues it and dedup makes the re-publish harmless.
			logger.Error("outbox: published row but failed to mark sent", "row_id", row.ID, "err", err)
		}
	}
	return nil
}

func (s *Scheduler) processSchedules(ctx context.Context) error {
	// Transaction
	tx, err := s.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. Fetch due schedules with SKIP LOCKED. Explicit column list (never SELECT *):
	// this is the dispatch path, so a new schedules column with a SELECT * would fail
	// the scan ("missing destination name X") and stop every scheduled launch (#91).
	query := `
		SELECT ` + store.ScheduleCols + `
		FROM schedules
		WHERE enabled = true AND next_run <= NOW()
		FOR UPDATE SKIP LOCKED
		LIMIT 10`

	var schedules []models.Schedule
	if err := tx.SelectContext(ctx, &schedules, query); err != nil {
		return fmt.Errorf("failed to select pending schedules: %w", err)
	}

	if len(schedules) == 0 {
		return nil
	}

	for _, sched := range schedules {
		logger.Info("processing schedule", "schedule_id", sched.ID, "name", sched.Name, "due_at", sched.NextRun)

		// 2. Launch the schedule's target — a workflow run or a job template —
		// carrying the schedule's own extra_vars into the job. (Previously these
		// were persisted on the schedule but silently dropped at launch; #79.)
		var opts launch.Options
		if len(sched.ExtraVars) > 0 {
			var ev map[string]interface{}
			if err := json.Unmarshal(sched.ExtraVars, &ev); err != nil {
				logger.Error("schedule extra_vars invalid; launching without them", "schedule_id", sched.ID, "err", err)
			} else {
				opts.ExtraVars = ev
			}
		}
		if err := launchTarget(ctx, tx, sched.Name, sched.WorkflowTemplateID, sched.UnifiedJobTemplateID, opts); err != nil {
			logger.Error("launch target for schedule failed", "schedule_id", sched.ID, "err", err)
			continue
		}
		logger.Info("launched target for schedule", "schedule_id", sched.ID)

		// 3. (Skipped) We do NOT create execution_run here.
		// The existing processPendingJobs loop picks up 'pending' jobs with no current_run_id and handles it.

		// 5. Calculate Next Run
		rule, err := rrule.StrToRRule(sched.RRule)
		if err != nil {
			logger.Error("invalid RRule for schedule; disabling", "schedule_id", sched.ID, "err", err)
			// Disable it to stop error loop
			logExec(ctx, tx, "UPDATE schedules SET enabled = false WHERE id = $1", sched.ID)
			continue
		}

		// rrule-go: rule.After(dt, inclusive)
		next := rule.After(time.Now(), false)

		logger.Info("schedule next run computed", "schedule_id", sched.ID, "next_run", next)

		_, err = tx.ExecContext(ctx, `
			UPDATE schedules 
			SET next_run = $1, modified_at = NOW() 
			WHERE id = $2`,
			next, sched.ID)

		if err != nil {
			logger.Error("update schedule next_run failed", "schedule_id", sched.ID, "err", err)
			continue
		}
	}

	return tx.Commit()
}

// processTimedOutJobs marks jobs that are stuck in running/queued state as failed.
// durationEnv reads a Go duration from an env var (e.g. "90s", "2m"), falling back
// to def if unset or unparseable. Used to tune the recovery/reconciliation windows
// without a rebuild.
func durationEnv(key string, def time.Duration) time.Duration {
	if v := env.String(key, ""); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		logger.Warn("ignoring unparseable duration env", "key", key, "value", v)
	}
	return def
}

// This catches cases where the host-runner crashes silently without sending events.
func (s *Scheduler) processTimedOutJobs(ctx context.Context) error {
	// Heartbeat-aware reconciliation. A long-running job is NOT failed merely for
	// running a long time; it is declared lost only when its liveness signal
	// disappears. The host-runner stamps execution_runs.last_heartbeat_at every
	// ~30s during execution, so:
	// All four windows are env-tunable (durations, e.g. "90s", "2m") so the recovery
	// cadence can be lowered in a lab or tightened in prod without a rebuild. Defaults
	// match the original hard-coded values.
	lostHeartbeatGrace := durationEnv("RECONCILE_HEARTBEAT_GRACE", 2*time.Minute) // ~4 missed heartbeats
	startGrace := durationEnv("RECONCILE_START_GRACE", 5*time.Minute)             // running but never heartbeated
	queuedTimeout := durationEnv("RECONCILE_QUEUED_TIMEOUT", 10*time.Minute)      // never picked up / never reported (parks remote runs to reconciling)
	localRecoveryGrace := durationEnv("RECONCILE_LOCAL_GRACE", 10*time.Minute)    // window for the executor to resume a local run before true-loss (#45)

	// 1a. Reconcilable runs: a REMOTE run (has a snapshotted runner host) whose
	// heartbeat went stale is NOT declared failed — the host may have finished the
	// job and hold the authoritative WAL that never got pushed. Hand it to the
	// pull-based reconciler by moving it to 'reconciling'; the job stays as-is
	// (not errored) until the reconciler resolves the true outcome. finished_at
	// stays NULL. See the reconciler service (github.com/praetordev/reconciler).
	staleCond := `(
		(er.last_heartbeat_at IS NOT NULL AND er.last_heartbeat_at < now() - $1::interval)
		OR (er.last_heartbeat_at IS NULL AND er.started_at IS NOT NULL AND er.started_at < now() - $2::interval)
	)`
	rec, err := s.DB.ExecContext(ctx, `
		UPDATE execution_runs er
		SET state = 'reconciling', reconcile_after = now()
		WHERE er.state = 'running' AND er.runner_host_id IS NOT NULL AND `+staleCond,
		fmt.Sprintf("%d seconds", int(lostHeartbeatGrace.Seconds())),
		fmt.Sprintf("%d seconds", int(startGrace.Seconds())),
	)
	if err != nil {
		logger.Error("move stale runs to reconciling failed", "err", err)
	} else if rows, _ := rec.RowsAffected(); rows > 0 {
		RunsReconciling.Add(float64(rows))
		logger.Info("moved stale remote runs to reconciling", "count", rows)
	}

	// Queue depth: jobs accepted but not yet running. Sampled once per tick.
	var depth float64
	if err := s.DB.GetContext(ctx, &depth,
		`SELECT count(*) FROM unified_jobs WHERE status IN ('pending','queued')`); err == nil {
		QueueDepth.Set(depth)
	}

	// 1b. Stale LOCAL runs (no runner host — ran on the executor itself). These
	// can't be pulled back over SSH, but the executor persists the same WAL to
	// /var/lib/praetor/jobs and resumes interrupted local runs on startup (#45), so
	// a stale local run is NOT immediately lost: park it in 'reconciling' with a
	// recovery deadline (reconcile_after) and leave its job untouched, giving the
	// executor time to resume it (a resumed runner's heartbeat revives it). The SSH
	// reconciler ignores these (it JOINs on runner_host_id), so parking is safe.
	rl, err := s.DB.ExecContext(ctx, `
		UPDATE execution_runs er
		SET state = 'reconciling', reconcile_after = now() + $3::interval
		WHERE er.state = 'running' AND er.runner_host_id IS NULL AND `+staleCond,
		fmt.Sprintf("%d seconds", int(lostHeartbeatGrace.Seconds())),
		fmt.Sprintf("%d seconds", int(startGrace.Seconds())),
		fmt.Sprintf("%d seconds", int(localRecoveryGrace.Seconds())),
	)
	if err != nil {
		logger.Error("park stale local runs for recovery failed", "err", err)
	} else if rows, _ := rl.RowsAffected(); rows > 0 {
		RunsReconciling.Add(float64(rows))
		logger.Info("parked stale local runs for executor recovery", "count", rows)
	}

	// 1c. True loss: a local run still in 'reconciling' past its recovery deadline
	// was never resumed (executor gone for good / WAL unrecoverable). Now declare
	// it lost and its job errored — the delayed form of the old 1b semantics.
	result, err := s.DB.ExecContext(ctx, `
		WITH lost AS (
			UPDATE execution_runs er
			SET state = 'lost', finished_at = now()
			WHERE er.state = 'reconciling' AND er.runner_host_id IS NULL
			  AND er.reconcile_after IS NOT NULL AND er.reconcile_after < now()
			RETURNING er.unified_job_id
		)
		UPDATE unified_jobs uj
		SET status = 'error', finished_at = now()
		FROM lost
		WHERE uj.id = lost.unified_job_id
		  AND uj.status NOT IN ('successful', 'failed', 'canceled', 'error')`)
	if err != nil {
		logger.Error("reconcile lost local runs failed", "err", err)
	} else if rows, _ := result.RowsAffected(); rows > 0 {
		RunsLost.Add(float64(rows))
		logger.Warn("marked local runs as lost (recovery deadline passed)", "count", rows)
	}

	// 2a. A REMOTE queued job stuck past the timeout must NOT be failed: it may have
	// actually run on its host with the outcome never pushed (ingestion down the
	// whole time, so no JOB_STARTED ever arrived to move it out of 'queued'). Failing
	// it here would be terminal and unrecoverable even after the WAL arrives. Instead
	// hand its run to the pull-based reconciler, which SSHes to the host and either
	// harvests the true outcome or (dir genuinely absent) declares it lost.
	if rec, err := s.DB.ExecContext(ctx, `
		UPDATE execution_runs er
		SET state = 'reconciling', reconcile_after = now()
		FROM unified_jobs uj
		WHERE uj.current_run_id = er.id
		  AND uj.status = 'queued'
		  AND uj.created_at < now() - $1::interval
		  AND er.runner_host_id IS NOT NULL
		  AND er.state <> 'reconciling' AND NOT run_is_terminal(er.state)`,
		fmt.Sprintf("%d seconds", int(queuedTimeout.Seconds())),
	); err != nil {
		logger.Error("park stuck remote queued runs failed", "err", err)
	} else if rows, _ := rec.RowsAffected(); rows > 0 {
		RunsReconciling.Add(float64(rows))
		logger.Info("parked stuck remote queued runs for reconciliation", "count", rows)
	}

	// 2b. A queued job with NO runner host was never dispatched to a target (a
	// genuine stuck-in-queue). With the durable outbox this is rare, but it's a safety
	// net and is safe to fail — there is no host holding a hidden outcome.
	result, err = s.DB.ExecContext(ctx, `
		WITH stuck AS (
			UPDATE unified_jobs uj
			SET status = 'failed', finished_at = now()
			WHERE uj.status = 'queued'
			  AND uj.current_run_id IS NOT NULL
			  AND uj.created_at < now() - $1::interval
			  AND NOT EXISTS (
			      SELECT 1 FROM execution_runs er
			      WHERE er.id = uj.current_run_id AND er.runner_host_id IS NOT NULL)
			RETURNING uj.current_run_id
		)
		UPDATE execution_runs er
		SET state = 'failed', finished_at = now()
		FROM stuck
		WHERE er.id = stuck.current_run_id
		  AND NOT run_is_terminal(er.state) AND er.state <> 'lost'`,
		fmt.Sprintf("%d seconds", int(queuedTimeout.Seconds())),
	)
	if err != nil {
		logger.Error("reconcile stuck queued jobs failed", "err", err)
	} else if rows, _ := result.RowsAffected(); rows > 0 {
		logger.Warn("marked queued jobs as failed (never dispatched, no host)", "count", rows)
	}

	// Void any still-pending outbox row whose run is already terminal. Without
	// this, a launch that was reaped above (or canceled) while its outbox row was
	// unsent — e.g. NATS was down so the relay never published — would be published
	// on recovery and bootstrap a "ghost run" the DB already calls failed. The
	// relay only picks status='pending', so flipping it to 'failed' retires it.
	if vr, verr := s.DB.ExecContext(ctx, `
		UPDATE execution_outbox o
		SET status = 'failed', attempts = attempts + 1
		FROM execution_runs er
		WHERE o.execution_run_id = er.id
		  AND o.status = 'pending'
		  AND er.state IN ('failed', 'canceled')`); verr != nil {
		logger.Error("void outbox for terminal runs failed", "err", verr)
	} else if n, _ := vr.RowsAffected(); n > 0 {
		logger.Info("voided pending outbox launches for already-terminal runs", "count", n)
	}

	return nil
}
