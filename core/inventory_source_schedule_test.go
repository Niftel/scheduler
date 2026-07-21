package core

import (
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

func TestScheduleActorCanSyncSourceFailsClosed(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	mock.ExpectBegin()
	tx, err := sqlx.NewDb(database, "sqlmock").BeginTxx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT EXISTS (")).WithArgs(int64(17), int64(23)).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	allowed, err := scheduleActorCanSyncSource(t.Context(), tx, 17, 23)
	if err != nil {
		t.Fatal(err)
	}
	if allowed {
		t.Fatal("revoked actor must not retain scheduled inventory-sync authority")
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
