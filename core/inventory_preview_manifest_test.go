package core

import (
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/praetordev/launch"
)

func TestBuildSyncManifestPreservesPreviewIntent(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	db := sqlx.NewDb(database, "sqlmock")
	mock.ExpectBegin()
	tx, err := db.BeginTxx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}

	mock.ExpectQuery("SELECT inventory_id, source, source_kind, credential_id FROM inventory_sources").
		WithArgs(int64(5)).
		WillReturnRows(sqlmock.NewRows([]string{"inventory_id", "source", "source_kind", "credential_id"}).
			AddRow(int64(7), "http://provider/inventory", "script", nil))

	opts := launch.ParseArgs(json.RawMessage(`{"inventory_source_id":5,"inventory_preview":true}`))
	manifest, credentialID, err := (&Scheduler{}).buildSyncManifest(t.Context(), tx, opts.InventorySourceID, opts.InventoryPreview)
	if err != nil {
		t.Fatalf("build sync manifest: %v", err)
	}
	if !manifest.InventoryPreview {
		t.Fatal("inventory preview intent was dropped from the execution manifest")
	}
	if credentialID != 0 {
		t.Fatalf("credential id = %d, want 0", credentialID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}
