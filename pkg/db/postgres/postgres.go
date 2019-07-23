package postgres

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"github.com/shasderias/sql-migrate/pkg/migrate"
)

func init() {
	migrate.RegisterDB("postgres", DB{})
}

type DB struct {
	*sqlx.DB
	tableName string
}

func (db DB) New(datasource, tableName string) (migrate.DB, error) {
	dbConn, err := sqlx.Open("postgres", datasource)
	if err != nil {
		return nil, err
	}

	return &DB{
		DB:        dbConn,
		tableName: tableName,
	}, nil
}

func (db DB) SqlDB() *sql.DB {
	return db.DB.DB
}

func (db DB) CreateRecordTable() error {
	const stmt = `
CREATE TABLE IF NOT EXISTS %s (
	id         TEXT        PRIMARY KEY,
	applied_at TIMESTAMPTZ NOT NULL
);`

	_, err := db.Exec(db.escapeTableName(stmt))
	return err
}

func (db DB) Records() ([]*migrate.Record, error) {
	const stmt = `
SELECT
	*
FROM 
	%s
ORDER BY id ASC;`

	var records []*migrate.Record

	if err := db.Select(&records, db.escapeTableName(stmt)); err != nil {
		return nil, err
	}

	return records, nil
}

const (
	insertRecordStmt = `INSERT INTO %s (id, applied_at) VALUES ($1, $2);`
	deleteRecordStmt = `DELETE FROM %s WHERE id = $1;`
)

func (db DB) InsertRecord(record *migrate.Record) error {
	_, err := db.Exec(db.escapeTableName(insertRecordStmt),
		record.ID, record.AppliedAt)

	return err
}

func (db DB) DeleteRecord(record *migrate.Record) error {
	_, err := db.Exec(db.escapeTableName(deleteRecordStmt),
		record.ID)

	return err
}

type Tx struct {
	*sqlx.Tx
	tableName string
}

func (db DB) Begin() (migrate.Tx, error) {
	tx, err := db.Beginx()
	if err != nil {
		return nil, err
	}

	return &Tx{
		Tx:        tx,
		tableName: db.tableName,
	}, nil
}

func (db DB) escapeTableName(stmt string) string {
	return fmt.Sprintf(stmt, pq.QuoteIdentifier(db.tableName))
}

func (tx Tx) InsertRecord(record *migrate.Record) error {
	_, err := tx.Exec(tx.escapeTableName(insertRecordStmt),
		record.ID, record.AppliedAt)

	return err
}

func (tx Tx) DeleteRecord(record *migrate.Record) error {
	_, err := tx.Exec(tx.escapeTableName(deleteRecordStmt),
		record.ID)

	return err
}

func (tx Tx) escapeTableName(stmt string) string {
	return fmt.Sprintf(stmt, pq.QuoteIdentifier(tx.tableName))
}
