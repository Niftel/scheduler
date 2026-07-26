package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/praetordev/launch"
	"github.com/praetordev/models"
)

func TestBuildJobManifestConsumesImmutableLaunchSnapshot(t *testing.T) {
	rawDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer rawDB.Close()
	db := sqlx.NewDb(rawDB, "sqlmock")
	scheduler := &Scheduler{DB: db}

	templateID := int64(7)
	templateInventoryID := int64(11)
	templateCredentialID := int64(13)
	selectedInventoryID := int64(21)
	selectedCredentialID := int64(23)
	selectedLimit := "selected-*"
	opts := launch.Options{
		SnapshotVersion:          launch.CurrentSnapshotVersion,
		InventoryID:              &selectedInventoryID,
		CredentialID:             &selectedCredentialID,
		SecretsCredentialID:      "11111111-2222-3333-4444-555555555555",
		SecretsCredentialVersion: 4,
		ExtraVars:                map[string]interface{}{"accepted": "value"},
		Limit:                    &selectedLimit,
	}
	job := models.UnifiedJob{
		ID:                   31,
		UnifiedJobTemplateID: &templateID,
		JobArgs:              opts.JobArgs(),
	}

	mock.ExpectBegin()
	tx, err := db.BeginTxx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	templateRows := sqlmock.NewRows([]string{
		"id", "organization_id", "name", "inventory_id", "project_id", "playbook",
		"credential_id", "execution_pack_id", "forks", "verbosity", "extra_vars",
		"job_limit", "use_fact_cache",
	}).AddRow(
		templateID, int64(3), "edited-template", templateInventoryID, nil, "site.yml",
		templateCredentialID, nil, 5, 0, []byte(`{"accepted":"edited","late":"value"}`),
		"edited-*", false,
	)
	mock.ExpectQuery("SELECT id, organization_id, name, inventory_id").
		WithArgs(templateID).
		WillReturnRows(templateRows)
	mock.ExpectQuery("SELECT id FROM inventories WHERE id = \\$1").
		WithArgs(selectedInventoryID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(selectedInventoryID))
	mock.ExpectQuery("SELECT id, name FROM hosts WHERE inventory_id = \\$1 AND is_runner_host").
		WithArgs(selectedInventoryID).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT id, name FROM hosts WHERE inventory_id = \\$1 AND enabled").
		WithArgs(selectedInventoryID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(41), "runner.example"))
	mock.ExpectQuery("SELECT c.name, c.inputs").
		WithArgs(int64(3)).
		WillReturnRows(sqlmock.NewRows([]string{"name", "inputs"}))

	manifest, runnerHostID, credential, err := scheduler.buildJobManifest(context.Background(), tx, job)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.InventoryID != selectedInventoryID || manifest.CredentialID != selectedCredentialID {
		t.Fatalf("manifest references = inventory %d credential %d, want %d/%d",
			manifest.InventoryID, manifest.CredentialID, selectedInventoryID, selectedCredentialID)
	}
	if manifest.Limit != selectedLimit {
		t.Fatalf("manifest limit = %q, want %q", manifest.Limit, selectedLimit)
	}
	wantVars := map[string]interface{}{"accepted": "value"}
	if got, _ := json.Marshal(manifest.ExtraVars); string(got) != `{"accepted":"value"}` {
		t.Fatalf("manifest vars = %s, want %v", got, wantVars)
	}
	if runnerHostID != 41 || !credential.Immutable || credential.ID != selectedCredentialID ||
		credential.SecretsVersion != 4 {
		t.Fatalf("resolved snapshot = runner %d credential %#v", runnerHostID, credential)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotRunCredentialUsesAcceptedSecretsVersionWithoutReread(t *testing.T) {
	rawDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer rawDB.Close()
	db := sqlx.NewDb(rawDB, "sqlmock")
	scheduler := &Scheduler{DB: db, SecretsIntegration: true}
	runID := uuid.New()
	serviceID := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	credential := credentialSnapshot{
		ID: 23, SecretsID: serviceID.String(), SecretsVersion: 4, Immutable: true,
	}

	mock.ExpectBegin()
	tx, err := db.BeginTxx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	mock.ExpectExec("UPDATE execution_runs").
		WithArgs(credential.ID, serviceID, credential.SecretsVersion, runID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := scheduler.snapshotRunCredential(context.Background(), tx, runID, credential); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
