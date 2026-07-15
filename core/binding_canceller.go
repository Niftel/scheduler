package core

import (
	"context"
	"fmt"
	"time"

	"github.com/Niftel/praetor-secrets/credential"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type BindingCancellationClient interface {
	CancelBinding(context.Context, credential.CancelBindingRequest) (credential.Binding, error)
}

type BindingCanceller struct {
	DB      *sqlx.DB
	Secrets BindingCancellationClient
	Now     func() time.Time
}

func NewBindingCanceller(database *sqlx.DB, secrets BindingCancellationClient) *BindingCanceller {
	return &BindingCanceller{DB: database, Secrets: secrets, Now: func() time.Time { return time.Now().UTC() }}
}

func (canceller *BindingCanceller) Start(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := canceller.RunOnce(ctx); err != nil && ctx.Err() == nil {
			logger.Error("credential binding cancellation pass failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (canceller *BindingCanceller) RunOnce(ctx context.Context) error {
	if canceller == nil || canceller.DB == nil || canceller.Secrets == nil || canceller.Now == nil {
		return fmt.Errorf("binding canceller is not configured")
	}
	var pending []struct {
		RunID      uuid.UUID `db:"id"`
		DispatchID uuid.UUID `db:"dispatch_id"`
		State      string    `db:"state"`
	}
	if err := canceller.DB.SelectContext(ctx, &pending, `SELECT id, dispatch_id, state
		FROM execution_runs
		WHERE run_is_terminal(state)
		  AND credential_binding_created_at IS NOT NULL
		  AND credential_binding_canceled_at IS NULL
		  AND dispatch_id IS NOT NULL
		ORDER BY finished_at NULLS LAST, id
		LIMIT 100`); err != nil {
		return fmt.Errorf("list pending binding cancellations: %w", err)
	}
	for _, run := range pending {
		reason := "run_" + run.State
		requestContext, cancel := context.WithTimeout(ctx, 15*time.Second)
		_, err := canceller.Secrets.CancelBinding(requestContext, credential.CancelBindingRequest{
			RunID: run.RunID.String(), DispatchID: run.DispatchID.String(), Reason: reason,
		})
		cancel()
		if err != nil {
			logger.Warn("credential binding cancellation deferred", "run_id", run.RunID, "err", err)
			continue
		}
		result, err := canceller.DB.ExecContext(ctx, `UPDATE execution_runs
			SET credential_binding_canceled_at = $1, credential_binding_cancel_reason = $2
			WHERE id = $3 AND dispatch_id = $4 AND credential_binding_canceled_at IS NULL`,
			canceller.Now(), reason, run.RunID, run.DispatchID)
		if err != nil {
			return fmt.Errorf("record binding cancellation for run %s: %w", run.RunID, err)
		}
		if rows, _ := result.RowsAffected(); rows == 1 {
			logger.Info("credential binding canceled", "run_id", run.RunID, "reason", reason)
		}
	}
	return nil
}
