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

// TestWorkflowLaunchArgsReachNodeJobs proves the workflow leg of the launch
// pipeline (#90): overrides handed to launch.Workflow are persisted on
// workflow_jobs.launch_args and, when the scheduler starts each node's job
// template, overlaid onto that node job's unified_jobs.job_args. Before this,
// launch.Workflow took no Options and every workflow-targeted schedule / webhook /
// EDA rule silently dropped its extra_vars, payload, and --limit.
//
// Requires TEST_DATABASE_URL (migrated); skips otherwise.
func TestWorkflowLaunchArgsReachNodeJobs(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping workflow launch-args integration test")
	}
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		t.Skipf("cannot reach TEST_DATABASE_URL: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	sched := NewScheduler(db, time.Second, nil)
	uniq := time.Now().UnixNano()

	// --- Fixture: org -> unified_job_template + job_template -> workflow_template
	// with one job node referencing that template. ---
	var orgID int64
	if err := db.QueryRow(`INSERT INTO organizations (name) VALUES ($1) RETURNING id`,
		fmt.Sprintf("wf-la-org-%d", uniq)).Scan(&orgID); err != nil {
		t.Fatalf("insert org: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM organizations WHERE id=$1`, orgID) })

	var ujtID int64
	if err := db.QueryRow(`INSERT INTO unified_job_templates (name) VALUES ($1) RETURNING id`,
		fmt.Sprintf("wf-la-ujt-%d", uniq)).Scan(&ujtID); err != nil {
		t.Fatalf("insert unified_job_template: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM unified_job_templates WHERE id=$1`, ujtID) })

	var jtID int64
	if err := db.QueryRow(
		`INSERT INTO job_templates (organization_id, name, playbook, unified_job_template_id)
		 VALUES ($1,$2,'site.yml',$3) RETURNING id`,
		orgID, fmt.Sprintf("wf-la-jt-%d", uniq), ujtID).Scan(&jtID); err != nil {
		t.Fatalf("insert job_template: %v", err)
	}

	var wtID int64
	if err := db.QueryRow(`INSERT INTO workflow_templates (organization_id, name) VALUES ($1,$2) RETURNING id`,
		orgID, fmt.Sprintf("wf-la-wt-%d", uniq)).Scan(&wtID); err != nil {
		t.Fatalf("insert workflow_template: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO workflow_nodes (workflow_template_id, node_key, node_type, job_template_id, name)
		 VALUES ($1,'n1','job',$2,'node one')`, wtID, jtID); err != nil {
		t.Fatalf("insert workflow_node: %v", err)
	}

	// --- Launch the workflow with overrides, exactly as a schedule/webhook/EDA
	// rule now does. ---
	limit := "web1"
	opts := launch.Options{
		ExtraVars: map[string]interface{}{"eda_event": map[string]interface{}{"host": "web1"}},
		Limit:     &limit,
	}
	wjID, err := launch.Workflow(ctx, db, wtID, opts)
	if err != nil {
		t.Fatalf("launch.Workflow: %v", err)
	}

	// The overrides must be persisted on the run itself.
	var storedArgs []byte
	if err := db.Get(&storedArgs, `SELECT launch_args FROM workflow_jobs WHERE id=$1`, wjID); err != nil {
		t.Fatalf("read workflow_jobs.launch_args: %v", err)
	}
	if got := launch.ParseArgs(storedArgs); got.EffectiveLimit("") != "web1" || got.ExtraVars["eda_event"] == nil {
		t.Fatalf("workflow_jobs.launch_args did not round-trip the overrides: %s", storedArgs)
	}

	// --- Advance the workflow once: the root job node launches as a unified_job. ---
	if err := sched.advanceWorkflow(ctx, wjID); err != nil {
		t.Fatalf("advanceWorkflow: %v", err)
	}

	var nodeJobID int64
	if err := db.Get(&nodeJobID,
		`SELECT unified_job_id FROM workflow_job_nodes WHERE workflow_job_id=$1 AND node_key='n1'`, wjID); err != nil {
		t.Fatalf("node did not launch a job (unified_job_id null?): %v", err)
	}

	// The node job must carry the workflow-level overrides in its job_args — this is
	// the whole point: the scheduler's manifest build then overlays them on the
	// node template's defaults (launch wins).
	var nodeArgs []byte
	if err := db.Get(&nodeArgs, `SELECT job_args FROM unified_jobs WHERE id=$1`, nodeJobID); err != nil {
		t.Fatalf("read node job job_args: %v", err)
	}
	got := launch.ParseArgs(nodeArgs)
	if got.EffectiveLimit("") != "web1" {
		t.Errorf("node job --limit = %q, want web1 (workflow limit dropped)", got.EffectiveLimit(""))
	}
	ev, ok := got.ExtraVars["eda_event"].(map[string]interface{})
	if !ok || ev["host"] != "web1" {
		t.Errorf("node job extra_vars.eda_event = %v, want {host: web1} (workflow extra_vars dropped)", got.ExtraVars["eda_event"])
	}

	// Sanity: an empty-Options launch stores "{}" and its node job carries no
	// overrides — the historical behavior for manual launches.
	wjID2, err := launch.Workflow(ctx, db, wtID, launch.Options{})
	if err != nil {
		t.Fatalf("launch.Workflow (empty): %v", err)
	}
	var stored2 []byte
	if err := db.Get(&stored2, `SELECT launch_args FROM workflow_jobs WHERE id=$1`, wjID2); err != nil {
		t.Fatalf("read empty launch_args: %v", err)
	}
	if s := string(stored2); s != "{}" {
		t.Errorf("empty-Options workflow launch_args = %q, want {}", s)
	}
}
