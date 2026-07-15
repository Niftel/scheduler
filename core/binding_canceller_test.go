package core

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Niftel/praetor-secrets/credential"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type cancellationClient struct {
	err     error
	request credential.CancelBindingRequest
}

func (client *cancellationClient) CancelBinding(_ context.Context, request credential.CancelBindingRequest) (credential.Binding, error) {
	client.request = request
	return credential.Binding{}, client.err
}

func newCancellationTest(t *testing.T) (*BindingCanceller, sqlmock.Sqlmock, *cancellationClient) {
	t.Helper()
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	client := &cancellationClient{}
	canceller := NewBindingCanceller(sqlx.NewDb(database, "sqlmock"), client)
	canceller.Now = func() time.Time { return time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC) }
	return canceller, mock, client
}

func TestBindingCancellerRecordsConfirmedCancellation(t *testing.T) {
	canceller, mock, client := newCancellationTest(t)
	runID, dispatchID := uuid.New(), uuid.New()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id, dispatch_id, state FROM execution_runs WHERE run_is_terminal(state) AND credential_binding_created_at IS NOT NULL AND credential_binding_canceled_at IS NULL AND dispatch_id IS NOT NULL ORDER BY finished_at NULLS LAST, id LIMIT 100")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "dispatch_id", "state"}).AddRow(runID, dispatchID, "successful"))
	mock.ExpectExec("UPDATE execution_runs").WithArgs(sqlmock.AnyArg(), "run_successful", runID, dispatchID).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := canceller.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if client.request.RunID != runID.String() || client.request.DispatchID != dispatchID.String() || client.request.Reason != "run_successful" {
		t.Fatalf("unexpected cancellation request: %+v", client.request)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBindingCancellerLeavesFailedCancellationPending(t *testing.T) {
	canceller, mock, client := newCancellationTest(t)
	client.err = errors.New("secrets unavailable")
	runID, dispatchID := uuid.New(), uuid.New()
	mock.ExpectQuery("SELECT id, dispatch_id, state").WillReturnRows(
		sqlmock.NewRows([]string{"id", "dispatch_id", "state"}).AddRow(runID, dispatchID, "failed"))
	if err := canceller.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
