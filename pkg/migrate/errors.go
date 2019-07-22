package migrate

import "fmt"

// PlanError happens where no migration plan could be created between the sets
// of already applied migrations and the currently found. For example, when the database
// contains a migration which is not among the migrations list found for an operation.
type PlanError struct {
	Migration *Migration
	Msg       string
}

func newPlanError(migration *Migration, msg string) error {
	return &PlanError{
		Migration: migration,
		Msg:       msg,
	}
}

func (p *PlanError) Error() string {
	return fmt.Sprintf("unable to create migration plan because of %s: %s",
		p.Migration.ID, p.Msg)
}

// TxError is returned when any error is encountered during a database
// transaction. It contains the relevant *Migration and notes it's ID in the
// Error function output.
type TxError struct {
	Migration *Migration
	Err       error
}

func newTxError(migration *PlannedMigration, err error) error {
	return &TxError{
		Migration: migration.Migration,
		Err:       err,
	}
}

func (e *TxError) Error() string {
	return e.Err.Error() + " handling " + e.Migration.ID
}
