package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lib/pq"

	"github.com/shasderias/sql-migrate/pkg/migrate"
)

func init() {
	migrate.RegisterDB("postgres", DB{})
}

type DB struct {
	*pgxpool.Pool
	tableName string
}

func (db DB) New(connString, tableName string) (migrate.DB, error) {
	conn, err := pgxpool.Connect(context.Background(), connString)
	if err != nil {
		return nil, err
	}

	return &DB{
		Pool:      conn,
		tableName: tableName,
	}, nil
}

func (db DB) CreateRecordTable() error {
	const stmt = `
CREATE TABLE IF NOT EXISTS %s (
	id         TEXT        PRIMARY KEY,
	applied_at TIMESTAMPTZ NOT NULL
);`

	_, err := db.Exec(context.Background(), db.escapeTableName(stmt))
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

	rows, err := db.Query(context.Background(), db.escapeTableName(stmt))
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var record migrate.Record
		err := rows.Scan(&record.ID, &record.AppliedAt)
		if err != nil {
			return nil, err
		}
		records = append(records, &record)
	}

	return records, nil
}

const (
	insertRecordStmt = `INSERT INTO %s (id, applied_at) VALUES ($1, $2);`
	deleteRecordStmt = `DELETE FROM %s WHERE id = $1;`
)

func (db DB) InsertRecord(record *migrate.Record) error {
	_, err := db.Exec(context.Background(), db.escapeTableName(insertRecordStmt),
		record.ID, record.AppliedAt)

	return err
}

func (db DB) DeleteRecord(record *migrate.Record) error {
	_, err := db.Exec(context.Background(), db.escapeTableName(deleteRecordStmt),
		record.ID)

	return err
}

type Tx struct {
	pgx.Tx
	tableName string
}

func (db DB) Begin() (migrate.Tx, error) {
	tx, err := db.BeginTx(context.Background(), pgx.TxOptions{
		IsoLevel: pgx.Serializable,
	})
	if err != nil {
		return nil, err
	}

	return &Tx{
		Tx:        tx,
		tableName: db.tableName,
	}, nil
}

func (db DB) escapeTableName(stmt string) string {
	return fmt.Sprintf(stmt, pgx.Identifier{db.tableName}.Sanitize())
}

func (db DB) Close() {
	db.Pool.Close()
}

func (tx Tx) InsertRecord(record *migrate.Record) error {
	_, err := tx.Exec(context.Background(), tx.escapeTableName(insertRecordStmt),
		record.ID, record.AppliedAt)

	return err
}

func (tx Tx) DeleteRecord(record *migrate.Record) error {
	_, err := tx.Exec(context.Background(), tx.escapeTableName(deleteRecordStmt),
		record.ID)

	return err
}

func (tx Tx) escapeTableName(stmt string) string {
	return fmt.Sprintf(stmt, pq.QuoteIdentifier(tx.tableName))
}

func (tx Tx) Commit() error {
	return tx.Tx.Commit(context.Background())
}

func (tx Tx) Rollback() error {
	return tx.Tx.Rollback(context.Background())
}
