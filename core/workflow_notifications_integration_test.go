package core

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/praetordev/launch"
)

// TestWorkflowNotificationsEnqueueDurably exercises the real PostgreSQL
// transition path. The target deliberately points at an unavailable endpoint:
// scheduling must depend only on durable database state, never destination
// availability or decrypted target configuration.
func TestWorkflowNotificationsEnqueueDurably(t *testing.T) {
	db := workflowNotificationTestDB(t)
	ctx := context.Background()
	uniq := time.Now().UnixNano()

	var orgID int64
	if err := db.QueryRow(
		`INSERT INTO organizations (name) VALUES ($1) RETURNING id`,
		fmt.Sprintf("wf-delivery-org-%d", uniq),
	).Scan(&orgID); err != nil {
		t.Fatalf("insert organization: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM organizations WHERE id=$1`, orgID) })

	assignedTeamID := insertNotificationTeam(t, db, orgID, fmt.Sprintf("assigned-%d", uniq))
	otherTeamID := insertNotificationTeam(t, db, orgID, fmt.Sprintf("other-%d", uniq))
	targetID := insertNotificationTarget(
		t, db, orgID, fmt.Sprintf("offline-target-%d", uniq), "webhook",
		`{"url":"http://127.0.0.1:1","credential":"must-not-be-read-by-scheduler"}`,
	)
	templateID := insertApprovalWorkflowTemplate(t, db, orgID, fmt.Sprintf("durable-workflow-%d", uniq), "gate-a", "gate-b")

	for _, event := range []string{"started", "success", "error"} {
		insertNotificationPolicy(t, db, orgID, nil, targetID, templateID, event)
	}
	for _, event := range []string{"approval", "approved", "denied", "timeout"} {
		insertNotificationPolicy(t, db, orgID, &assignedTeamID, targetID, templateID, event)
		insertNotificationPolicy(t, db, orgID, &otherTeamID, targetID, templateID, event)
	}

	scheduler := NewScheduler(db, time.Second, nil)

	t.Run("assigned team, distinct nodes, deduplication, and restart", func(t *testing.T) {
		runID := launchNotificationWorkflow(t, ctx, db, templateID, assignedTeamID)
		advanceNotificationWorkflow(t, scheduler, ctx, runID, "start")

		assertDeliveryCount(t, db, runID, "started", nil, 1)
		assertDeliveryCount(t, db, runID, "approval", &assignedTeamID, 2)
		assertDeliveryCount(t, db, runID, "approval", &otherTeamID, 0)
		assertDistinctOccurrences(t, db, runID, "approval", 2)

		// Reprocessing and a process restart must both leave the logical
		// occurrences unchanged.
		advanceNotificationWorkflow(t, scheduler, ctx, runID, "duplicate advancement")
		restarted := NewScheduler(db, time.Second, nil)
		advanceNotificationWorkflow(t, restarted, ctx, runID, "restart advancement")
		assertDeliveryCount(t, db, runID, "started", nil, 1)
		assertDeliveryCount(t, db, runID, "approval", &assignedTeamID, 2)

		if _, err := db.Exec(`
			UPDATE workflow_job_nodes
			SET status='approved'
			WHERE workflow_job_id=$1 AND node_key IN ('gate-a','gate-b')`, runID); err != nil {
			t.Fatalf("approve workflow nodes: %v", err)
		}
		advanceNotificationWorkflow(t, restarted, ctx, runID, "approval completion")
		assertDeliveryCount(t, db, runID, "approved", &assignedTeamID, 2)
		assertDeliveryCount(t, db, runID, "approved", &otherTeamID, 0)
		assertDeliveryCount(t, db, runID, "success", nil, 1)
		assertWorkflowStatus(t, db, runID, "successful")
	})

	t.Run("denied outcome", func(t *testing.T) {
		runID := launchNotificationWorkflow(t, ctx, db, templateID, assignedTeamID)
		advanceNotificationWorkflow(t, scheduler, ctx, runID, "start")
		if _, err := db.Exec(`
			UPDATE workflow_job_nodes
			SET status=CASE node_key WHEN 'gate-a' THEN 'rejected' ELSE 'approved' END
			WHERE workflow_job_id=$1`, runID); err != nil {
			t.Fatalf("decide workflow nodes: %v", err)
		}
		advanceNotificationWorkflow(t, scheduler, ctx, runID, "denied completion")
		assertDeliveryCount(t, db, runID, "denied", &assignedTeamID, 1)
		assertDeliveryCount(t, db, runID, "approved", &assignedTeamID, 1)
		assertDeliveryCount(t, db, runID, "error", nil, 1)
		assertWorkflowStatus(t, db, runID, "failed")
	})

	t.Run("timeout outcome", func(t *testing.T) {
		runID := launchNotificationWorkflow(t, ctx, db, templateID, assignedTeamID)
		advanceNotificationWorkflow(t, scheduler, ctx, runID, "start")
		if _, err := db.Exec(`
			UPDATE workflow_job_nodes
			SET awaiting_since=CASE
				WHEN node_key='gate-a' THEN now()-interval '25 hours'
				ELSE awaiting_since
			END,
			status=CASE WHEN node_key='gate-b' THEN 'approved' ELSE status END
			WHERE workflow_job_id=$1`, runID); err != nil {
			t.Fatalf("prepare timeout workflow: %v", err)
		}
		advanceNotificationWorkflow(t, scheduler, ctx, runID, "timeout completion")
		assertDeliveryCount(t, db, runID, "timeout", &assignedTeamID, 1)
		assertDeliveryCount(t, db, runID, "approved", &assignedTeamID, 1)
		assertDeliveryCount(t, db, runID, "error", nil, 1)
		assertWorkflowStatus(t, db, runID, "failed")

		var audit struct {
			Status          string `db:"status"`
			TimedOut        bool   `db:"timed_out"`
			OutcomeNotified bool   `db:"outcome_notified"`
		}
		if err := db.Get(&audit, `
			SELECT status, timed_out, outcome_notified
			FROM workflow_job_nodes
			WHERE workflow_job_id=$1 AND node_key='gate-a'`, runID); err != nil {
			t.Fatalf("read timeout audit: %v", err)
		}
		if audit.Status != "rejected" || !audit.TimedOut || !audit.OutcomeNotified {
			t.Fatalf("unexpected timeout audit: %+v", audit)
		}
	})

	t.Run("preexisting timeout is recovered after restart", func(t *testing.T) {
		runID := launchNotificationWorkflow(t, ctx, db, templateID, assignedTeamID)
		advanceNotificationWorkflow(t, scheduler, ctx, runID, "start")
		if _, err := db.Exec(`
			UPDATE workflow_job_nodes
			SET status=CASE node_key WHEN 'gate-a' THEN 'rejected' ELSE 'approved' END,
			    timed_out=(node_key='gate-a'),
			    outcome_notified=false
			WHERE workflow_job_id=$1`, runID); err != nil {
			t.Fatalf("prepare preexisting timeout: %v", err)
		}

		restarted := NewScheduler(db, time.Second, nil)
		advanceNotificationWorkflow(t, restarted, ctx, runID, "recover preexisting timeout")
		assertDeliveryCount(t, db, runID, "timeout", &assignedTeamID, 1)
		assertDeliveryCount(t, db, runID, "approved", &assignedTeamID, 1)
		assertDeliveryCount(t, db, runID, "error", nil, 1)
		assertWorkflowStatus(t, db, runID, "failed")
	})
}

// TestWorkflowNotificationEnqueueFailureRollsBackTransition proves an enqueue
// failure cannot strand a state watermark ahead of its delivery. A deliberately
// overlong target type is legal in the legacy target table but rejected by the
// bounded delivery snapshot, forcing the INSERT and the surrounding transition
// transaction to roll back.
func TestWorkflowNotificationEnqueueFailureRollsBackTransition(t *testing.T) {
	db := workflowNotificationTestDB(t)
	ctx := context.Background()
	uniq := time.Now().UnixNano()

	var orgID int64
	if err := db.QueryRow(
		`INSERT INTO organizations (name) VALUES ($1) RETURNING id`,
		fmt.Sprintf("wf-rollback-org-%d", uniq),
	).Scan(&orgID); err != nil {
		t.Fatalf("insert organization: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM organizations WHERE id=$1`, orgID) })

	teamID := insertNotificationTeam(t, db, orgID, fmt.Sprintf("rollback-team-%d", uniq))
	targetID := insertNotificationTarget(
		t, db, orgID, fmt.Sprintf("rollback-target-%d", uniq),
		strings.Repeat("x", 65), `{}`,
	)
	templateID := insertApprovalWorkflowTemplate(
		t, db, orgID, fmt.Sprintf("rollback-workflow-%d", uniq), "gate",
	)
	insertNotificationPolicy(t, db, orgID, &teamID, targetID, templateID, "approval")

	runID := launchNotificationWorkflow(t, ctx, db, templateID, teamID)
	scheduler := NewScheduler(db, time.Second, nil)
	if err := scheduler.advanceWorkflow(ctx, runID); err == nil {
		t.Fatal("advanceWorkflow succeeded; want bounded delivery snapshot failure")
	}

	var state struct {
		Status          string `db:"status"`
		StartedNotified bool   `db:"started_notified"`
	}
	if err := db.Get(&state, `
		SELECT wjn.status, wj.started_notified
		FROM workflow_jobs wj
		JOIN workflow_job_nodes wjn ON wjn.workflow_job_id=wj.id
		WHERE wj.id=$1`, runID); err != nil {
		t.Fatalf("read rolled-back state: %v", err)
	}
	if state.Status != "pending" {
		t.Fatalf("approval node status=%q, want pending after enqueue rollback", state.Status)
	}
	if !state.StartedNotified {
		t.Fatal("independent started watermark was unexpectedly rolled back")
	}
	assertDeliveryCount(t, db, runID, "approval", &teamID, 0)
}

func workflowNotificationTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping workflow notification integration test")
	}
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		t.Skipf("cannot reach TEST_DATABASE_URL: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func insertNotificationTeam(t *testing.T, db *sqlx.DB, orgID int64, name string) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(
		`INSERT INTO teams (organization_id,name) VALUES ($1,$2) RETURNING id`,
		orgID, name,
	).Scan(&id); err != nil {
		t.Fatalf("insert team: %v", err)
	}
	return id
}

func insertNotificationTarget(
	t *testing.T,
	db *sqlx.DB,
	orgID int64,
	name string,
	targetType string,
	config string,
) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`
		INSERT INTO notification_templates (organization_id,name,notification_type,config)
		VALUES ($1,$2,$3,$4::jsonb)
		RETURNING id`, orgID, name, targetType, config).Scan(&id); err != nil {
		t.Fatalf("insert notification target: %v", err)
	}
	return id
}

func insertApprovalWorkflowTemplate(
	t *testing.T,
	db *sqlx.DB,
	orgID int64,
	name string,
	nodeKeys ...string,
) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(
		`INSERT INTO workflow_templates (organization_id,name) VALUES ($1,$2) RETURNING id`,
		orgID, name,
	).Scan(&id); err != nil {
		t.Fatalf("insert workflow template: %v", err)
	}
	for _, nodeKey := range nodeKeys {
		if _, err := db.Exec(`
			INSERT INTO workflow_nodes (
				workflow_template_id,node_key,node_type,name,
				approval_timeout_seconds,approval_timeout_action
			)
			VALUES ($1,$2,'approval',$2,86400,'rejected')`, id, nodeKey); err != nil {
			t.Fatalf("insert approval node %q: %v", nodeKey, err)
		}
	}
	return id
}

func insertNotificationPolicy(
	t *testing.T,
	db *sqlx.DB,
	orgID int64,
	teamID *int64,
	targetID int64,
	templateID int64,
	event string,
) {
	t.Helper()
	if _, err := db.Exec(`
		INSERT INTO notification_policies (
			organization_id,team_id,notification_template_id,
			resource_type,resource_id,event
		)
		VALUES ($1,$2,$3,'workflow_template',$4,$5)`,
		orgID, teamID, targetID, templateID, event); err != nil {
		t.Fatalf("insert %s notification policy: %v", event, err)
	}
}

func launchNotificationWorkflow(
	t *testing.T,
	ctx context.Context,
	db *sqlx.DB,
	templateID int64,
	teamID int64,
) int64 {
	t.Helper()
	runID, err := launch.Workflow(ctx, db, templateID, launch.Options{})
	if err != nil {
		t.Fatalf("launch workflow: %v", err)
	}
	if _, err := db.Exec(
		`UPDATE workflow_jobs SET approval_team_id=$1 WHERE id=$2`,
		teamID, runID,
	); err != nil {
		t.Fatalf("assign approval team: %v", err)
	}
	return runID
}

func advanceNotificationWorkflow(
	t *testing.T,
	scheduler *Scheduler,
	ctx context.Context,
	runID int64,
	phase string,
) {
	t.Helper()
	if err := scheduler.advanceWorkflow(ctx, runID); err != nil {
		t.Fatalf("advance workflow (%s): %v", phase, err)
	}
}

func assertDeliveryCount(
	t *testing.T,
	db *sqlx.DB,
	runID int64,
	event string,
	teamID *int64,
	want int,
) {
	t.Helper()
	var got int
	if err := db.Get(&got, `
		SELECT count(*)
		FROM notification_deliveries
		WHERE subject_id=$1
		  AND event=$2
		  AND team_id IS NOT DISTINCT FROM $3`, runID, event, teamID); err != nil {
		t.Fatalf("count %s deliveries: %v", event, err)
	}
	if got != want {
		t.Fatalf("%s deliveries for team %v = %d, want %d", event, teamID, got, want)
	}
}

func assertDistinctOccurrences(
	t *testing.T,
	db *sqlx.DB,
	runID int64,
	event string,
	want int,
) {
	t.Helper()
	var got int
	if err := db.Get(&got, `
		SELECT count(DISTINCT occurrence_id)
		FROM notification_deliveries
		WHERE subject_id=$1 AND event=$2`, runID, event); err != nil {
		t.Fatalf("count distinct %s occurrences: %v", event, err)
	}
	if got != want {
		t.Fatalf("distinct %s occurrences=%d, want %d", event, got, want)
	}
}

func assertWorkflowStatus(t *testing.T, db *sqlx.DB, runID int64, want string) {
	t.Helper()
	var got string
	if err := db.Get(&got, `SELECT status FROM workflow_jobs WHERE id=$1`, runID); err != nil {
		t.Fatalf("read workflow status: %v", err)
	}
	if got != want {
		t.Fatalf("workflow %d status=%q, want %q", runID, got, want)
	}
}
