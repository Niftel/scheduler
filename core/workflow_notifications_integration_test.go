package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/praetordev/launch"
)

// TestWorkflowNotificationsFire proves the scheduler dispatches every workflow
// notification event exactly where its transition happens: 'started' on first
// advance, 'approval' when an approval node starts waiting, 'approved'/'denied' on
// the approval outcome, and 'success'/'error' on terminal finalize. It stands up a
// real HTTP receiver (the webhook backend) and asserts each delivery carries the
// right event + workflow kind.
//
// Requires TEST_DATABASE_URL (migrated); skips otherwise.
func TestWorkflowNotificationsFire(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping workflow notification integration test")
	}
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		t.Skipf("cannot reach TEST_DATABASE_URL: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	sched := NewScheduler(db, time.Second, nil)
	uniq := time.Now().UnixNano()

	// HTTP receiver for the webhook backend; each delivery is pushed to got.
	got := make(chan map[string]interface{}, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]interface{}
		_ = json.Unmarshal(b, &m)
		got <- m
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// --- Fixture: org, a webhook notification template pointing at srv, a workflow
	// template with a single approval node, and an attachment for every event. ---
	var orgID int64
	if err := db.QueryRow(`INSERT INTO organizations (name) VALUES ($1) RETURNING id`,
		fmt.Sprintf("wf-notif-org-%d", uniq)).Scan(&orgID); err != nil {
		t.Fatalf("insert org: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM organizations WHERE id=$1`, orgID) })

	// Config is stored as plaintext {"url":...}; DecryptConfig passes through values
	// that aren't ciphertext, so no crypto setup is needed in the test.
	cfg, _ := json.Marshal(map[string]string{"url": srv.URL})
	var ntID int64
	if err := db.QueryRow(
		`INSERT INTO notification_templates (organization_id, name, notification_type, config)
		 VALUES ($1,$2,'webhook',$3) RETURNING id`,
		orgID, fmt.Sprintf("wf-notif-nt-%d", uniq), cfg).Scan(&ntID); err != nil {
		t.Fatalf("insert notification_template: %v", err)
	}

	var wtID int64
	if err := db.QueryRow(`INSERT INTO workflow_templates (organization_id, name) VALUES ($1,$2) RETURNING id`,
		orgID, fmt.Sprintf("wf-notif-wt-%d", uniq)).Scan(&wtID); err != nil {
		t.Fatalf("insert workflow_template: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO workflow_nodes (workflow_template_id, node_key, node_type, name)
		 VALUES ($1,'gate','approval','the gate')`, wtID); err != nil {
		t.Fatalf("insert approval node: %v", err)
	}
	for _, ev := range []string{"started", "success", "error", "approval", "approved", "denied"} {
		if _, err := db.Exec(
			`INSERT INTO workflow_template_notifications (workflow_template_id, notification_template_id, event)
			 VALUES ($1,$2,$3)`, wtID, ntID, ev); err != nil {
			t.Fatalf("attach %s notification: %v", ev, err)
		}
	}

	// seen records every delivery (event -> kind). expect drains until the wanted
	// event has been seen, recording others along the way (events from two runs
	// share the channel, and 'started'/'approval' recur).
	seen := map[string]string{}
	expect := func(t *testing.T, wantEvent, wantKind string) {
		t.Helper()
		for {
			if k, ok := seen[wantEvent]; ok {
				if k != wantKind {
					t.Fatalf("%s delivered with kind=%q, want %q", wantEvent, k, wantKind)
				}
				return
			}
			select {
			case m := <-got:
				ev, _ := m["event"].(string)
				kind, _ := m["kind"].(string)
				seen[ev] = kind
			case <-time.After(3 * time.Second):
				t.Fatalf("timed out waiting for %q notification (seen=%v)", wantEvent, seen)
			}
		}
	}
	advance := func(t *testing.T, wjID int64, phase string) {
		t.Helper()
		if err := sched.advanceWorkflow(ctx, wjID); err != nil {
			t.Fatalf("advanceWorkflow (%s): %v", phase, err)
		}
	}

	// --- Approve path: started + approval, then approved + success. ---
	wjA, err := launch.Workflow(ctx, db, wtID, launch.Options{})
	if err != nil {
		t.Fatalf("launch.Workflow A: %v", err)
	}
	advance(t, wjA, "A start")
	expect(t, "started", "workflow")
	expect(t, "approval", "workflow approval")

	// Once-only: advancing again while the node is still awaiting approval must not
	// re-deliver 'started' or 'approval' (the whole point of the watermarks).
	advance(t, wjA, "A idle re-advance")
	select {
	case m := <-got:
		t.Fatalf("duplicate notification on idle re-advance: %v", m)
	case <-time.After(500 * time.Millisecond):
	}

	if _, err := db.Exec(`UPDATE workflow_job_nodes SET status='approved' WHERE workflow_job_id=$1 AND node_key='gate'`, wjA); err != nil {
		t.Fatalf("approve node: %v", err)
	}
	advance(t, wjA, "A finalize")
	expect(t, "approved", "workflow approval")
	expect(t, "success", "workflow")
	assertWFStatus(t, db, wjA, "successful")

	// --- Deny path: a fresh run, then denied + error. (started/approval recur and
	// are drained by expect.) ---
	seen = map[string]string{} // reset so recurring started/approval are re-observed
	wjB, err := launch.Workflow(ctx, db, wtID, launch.Options{})
	if err != nil {
		t.Fatalf("launch.Workflow B: %v", err)
	}
	advance(t, wjB, "B start")
	expect(t, "started", "workflow")
	expect(t, "approval", "workflow approval")

	if _, err := db.Exec(`UPDATE workflow_job_nodes SET status='rejected' WHERE workflow_job_id=$1 AND node_key='gate'`, wjB); err != nil {
		t.Fatalf("deny node: %v", err)
	}
	advance(t, wjB, "B finalize")
	expect(t, "denied", "workflow approval")
	expect(t, "error", "workflow")
	assertWFStatus(t, db, wjB, "failed")
}

func assertWFStatus(t *testing.T, db *sqlx.DB, wjID int64, want string) {
	t.Helper()
	var status string
	if err := db.Get(&status, `SELECT status FROM workflow_jobs WHERE id=$1`, wjID); err != nil {
		t.Fatalf("read workflow status: %v", err)
	}
	if status != want {
		t.Fatalf("workflow %d status = %q, want %q", wjID, status, want)
	}
}
