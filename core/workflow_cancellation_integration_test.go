package core

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/praetordev/launch"
)

// TestCanceledNodeJobFinalizesWorkflow reproduces the cancellation propagation
// gap: the API cancels a pending/queued unified job directly, then the scheduler
// must reap that terminal state into both the node and its parent workflow.
func TestCanceledNodeJobFinalizesWorkflow(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping workflow cancellation integration test")
	}
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		t.Skipf("cannot reach TEST_DATABASE_URL: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	sched := NewScheduler(db, time.Second, nil)
	uniq := time.Now().UnixNano()

	var orgID int64
	if err := db.QueryRow(`INSERT INTO organizations (name) VALUES ($1) RETURNING id`,
		fmt.Sprintf("wf-cancel-org-%d", uniq)).Scan(&orgID); err != nil {
		t.Fatalf("insert org: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM organizations WHERE id=$1`, orgID) })

	var ujtID int64
	if err := db.QueryRow(`INSERT INTO unified_job_templates (name) VALUES ($1) RETURNING id`,
		fmt.Sprintf("wf-cancel-ujt-%d", uniq)).Scan(&ujtID); err != nil {
		t.Fatalf("insert unified job template: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM unified_job_templates WHERE id=$1`, ujtID) })

	var jtID int64
	if err := db.QueryRow(
		`INSERT INTO job_templates (organization_id, name, playbook, unified_job_template_id)
		 VALUES ($1,$2,'site.yml',$3) RETURNING id`,
		orgID, fmt.Sprintf("wf-cancel-jt-%d", uniq), ujtID).Scan(&jtID); err != nil {
		t.Fatalf("insert job template: %v", err)
	}

	var wtID int64
	if err := db.QueryRow(`INSERT INTO workflow_templates (organization_id, name) VALUES ($1,$2) RETURNING id`,
		orgID, fmt.Sprintf("wf-cancel-wt-%d", uniq)).Scan(&wtID); err != nil {
		t.Fatalf("insert workflow template: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO workflow_nodes (workflow_template_id, node_key, node_type, job_template_id, name)
		 VALUES ($1,'job','job',$2,'cancelable job')`, wtID, jtID); err != nil {
		t.Fatalf("insert workflow node: %v", err)
	}

	wjID, err := launch.Workflow(ctx, db, wtID, launch.Options{})
	if err != nil {
		t.Fatalf("launch workflow: %v", err)
	}
	if err := sched.advanceWorkflow(ctx, wjID); err != nil {
		t.Fatalf("launch node job: %v", err)
	}

	var nodeJobID int64
	if err := db.Get(&nodeJobID,
		`SELECT unified_job_id FROM workflow_job_nodes WHERE workflow_job_id=$1 AND node_key='job'`, wjID); err != nil {
		t.Fatalf("read node job: %v", err)
	}
	if _, err := db.Exec(`UPDATE unified_jobs SET status='canceled', finished_at=now() WHERE id=$1`, nodeJobID); err != nil {
		t.Fatalf("cancel node job: %v", err)
	}
	if err := sched.advanceWorkflow(ctx, wjID); err != nil {
		t.Fatalf("reap canceled node job: %v", err)
	}

	var nodeStatus, workflowStatus string
	if err := db.Get(&nodeStatus,
		`SELECT status FROM workflow_job_nodes WHERE workflow_job_id=$1 AND node_key='job'`, wjID); err != nil {
		t.Fatalf("read node status: %v", err)
	}
	if err := db.Get(&workflowStatus, `SELECT status FROM workflow_jobs WHERE id=$1`, wjID); err != nil {
		t.Fatalf("read workflow status: %v", err)
	}
	if nodeStatus != "canceled" || workflowStatus != "canceled" {
		t.Fatalf("cancellation did not propagate: node=%s workflow=%s", nodeStatus, workflowStatus)
	}
}
