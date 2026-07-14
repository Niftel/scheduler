package core

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/praetordev/eventbus"
	"github.com/praetordev/events"
)

// TestOutboxRelayDeliversDurably proves item 6b end-to-end: a committed outbox
// row is published by the relay, marked sent, and durably delivered to an
// executor subscriber.
//
// Requires TEST_DATABASE_URL (migrated, incl. execution_outbox) and a
// JetStream TEST_NATS_URL; skips otherwise.
func TestOutboxRelayDeliversDurably(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	natsURL := os.Getenv("TEST_NATS_URL")
	if dbURL == "" || natsURL == "" {
		t.Skip("TEST_DATABASE_URL and TEST_NATS_URL required; skipping relay integration test")
	}

	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		t.Skipf("cannot reach TEST_DATABASE_URL: %v", err)
	}
	defer db.Close()

	bus, err := eventbus.NewBus(natsURL)
	if err != nil {
		t.Skipf("cannot reach JetStream NATS: %v", err)
	}
	defer bus.Close()

	sched := NewScheduler(db, time.Second, bus)

	// --- Fixture: job + run + a pending outbox row ---
	var jobID int64
	if err := db.QueryRow(
		`INSERT INTO unified_jobs (name, status) VALUES ('relay-test', 'queued') RETURNING id`,
	).Scan(&jobID); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	runID := uuid.New()
	if _, err := db.Exec(
		`INSERT INTO execution_runs (id, unified_job_id, state) VALUES ($1, $2, 'pending')`,
		runID, jobID,
	); err != nil {
		t.Fatalf("insert run: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM unified_jobs WHERE id = $1`, jobID) })

	payload, _ := json.Marshal(&events.ExecutionRequest{ExecutionRunID: runID, UnifiedJobID: jobID})
	var outboxID int64
	if err := db.QueryRow(
		`INSERT INTO execution_outbox (execution_run_id, payload) VALUES ($1, $2) RETURNING id`,
		runID, payload,
	).Scan(&outboxID); err != nil {
		t.Fatalf("insert outbox: %v", err)
	}

	// --- Relay ---
	if err := sched.relayOutbox(context.Background()); err != nil {
		t.Fatalf("relayOutbox: %v", err)
	}

	var status string
	if err := db.Get(&status, `SELECT status FROM execution_outbox WHERE id = $1`, outboxID); err != nil {
		t.Fatalf("get outbox status: %v", err)
	}
	if status != "sent" {
		t.Fatalf("expected outbox row 'sent', got %q", status)
	}

	// The relayed launch must be durably delivered to an executor subscriber.
	ch, err := bus.SubscribeToExecutionRequests()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case got := <-ch:
		if got.ExecutionRunID != runID {
			t.Fatalf("delivered wrong run %s (want %s)", got.ExecutionRunID, runID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("relayed launch was not delivered")
	}
}
