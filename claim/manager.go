package claim

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"

	"github.com/Niftel/praetor-secrets/credential"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

var (
	ErrNotFound = errors.New("execution run not found")
	ErrConflict = errors.New("execution run claim conflict")
)

type BindingClient interface {
	RegisterBinding(context.Context, credential.RegisterBindingRequest) (credential.Binding, error)
}

type Manager struct {
	DB      *sqlx.DB
	Secrets BindingClient
	Now     func() time.Time
}

func NewManager(db *sqlx.DB, secrets BindingClient) *Manager {
	return &Manager{DB: db, Secrets: secrets, Now: func() time.Time { return time.Now().UTC() }}
}

func (m *Manager) Claim(ctx context.Context, runID, dispatchID uuid.UUID, executorIdentity string) error {
	if m.DB == nil || m.Secrets == nil || runID == uuid.Nil || dispatchID == uuid.Nil || executorIdentity == "" {
		return ErrConflict
	}
	tx, err := m.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var row struct {
		State             string         `db:"state"`
		DispatchID        uuid.UUID      `db:"dispatch_id"`
		Executor          sql.NullString `db:"executor_identity"`
		CredentialID      *uuid.UUID     `db:"secrets_credential_id"`
		CredentialVersion *int64         `db:"secrets_credential_version"`
		OrganizationID    *int64         `db:"organization_id"`
	}
	err = tx.GetContext(ctx, &row, `SELECT er.state, er.dispatch_id, er.executor_identity,
		er.secrets_credential_id, er.secrets_credential_version, c.organization_id
		FROM execution_runs er LEFT JOIN credentials c ON c.id = er.credential_id
		WHERE er.id = $1 FOR UPDATE OF er`, runID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if row.DispatchID != dispatchID {
		return ErrConflict
	}
	if row.Executor.Valid {
		if row.Executor.String != executorIdentity {
			return ErrConflict
		}
		return tx.Commit()
	}
	if row.State != "pending" {
		return ErrConflict
	}
	now := m.Now()
	if row.CredentialID != nil {
		if row.CredentialVersion == nil || *row.CredentialVersion <= 0 || row.OrganizationID == nil {
			return ErrConflict
		}
		_, err = m.Secrets.RegisterBinding(ctx, credential.RegisterBindingRequest{
			RunID: runID.String(), DispatchID: dispatchID.String(), OrganizationID: strconv.FormatInt(*row.OrganizationID, 10),
			CredentialID: row.CredentialID.String(), ExecutorIdentity: executorIdentity,
			NotBefore: now, ExpiresAt: now.Add(24 * time.Hour), MaxResolutions: 3, IdempotencyKey: dispatchID.String(),
		})
		if err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE execution_runs SET executor_identity = $1,
		credential_binding_created_at = $2 WHERE id = $3 AND executor_identity IS NULL`, executorIdentity, now, runID)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return ErrConflict
	}
	return tx.Commit()
}
