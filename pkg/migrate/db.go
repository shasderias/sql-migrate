package migrate

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgconn"
)

type Record struct {
	ID        string    `db:"id"`
	AppliedAt time.Time `db:"applied_at"`
}

var supportedDialects = map[string]DB{}

func getDB(dialect, datasource, tableName string) (DB, error) {
	d, ok := supportedDialects[dialect]
	if !ok {
		return nil, fmt.Errorf("unsupported dialect: %s", dialect)
	}

	db, err := d.New(datasource, tableName)
	if err != nil {
		return nil, fmt.Errorf("error connecting to DB: %s", err)
	}

	if err := db.CreateRecordTable(); err != nil {
		return nil, err
	}

	return db, nil
}

func RegisterDB(dialect string, db DB) {
	supportedDialects[dialect] = db
}

type DB interface {
	SqlExecutor

	New(datasource, tableName string) (DB, error)
	CreateRecordTable() error
	Records() ([]*Record, error)
	Begin() (Tx, error)
	Close()
}

type Tx interface {
	SqlExecutor
	Commit() error
	Rollback() error
}

type SqlExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	InsertRecord(record *Record) error
	DeleteRecord(record *Record) error
}
