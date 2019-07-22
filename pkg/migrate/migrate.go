package migrate

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/shasderias/sql-migrate/pkg/sqlparse"
)

type Direction int

const (
	Up Direction = iota
	Down
)

type Record struct {
	ID        string    `db:"id"`
	AppliedAt time.Time `db:"applied_at"`
}

// Migration parsing
func parse(id string, r io.ReadSeeker) (*Migration, error) {
	m := &Migration{
		ID: id,
	}

	parsed, err := sqlparse.ParseMigration(r)
	if err != nil {
		return nil, fmt.Errorf("error parsing migration (%s): %s", id, err)
	}

	m.Up = parsed.UpStatements
	m.Down = parsed.DownStatements

	m.DisableTransactionUp = parsed.DisableTransactionUp
	m.DisableTransactionDown = parsed.DisableTransactionDown

	return m, nil
}

// Exec executes a set of migrations and returns the number of applied migrations.
func Exec(db DB, m Source, dir Direction) (int, error) {
	return ExecMax(db, m, dir, 0)
}

// ExecMax executes a set of migrations, up to a maximum of max migrations, and
// returns the number of applied migrations.
//
// Pass 0 for no limit (or use Exec).
func ExecMax(db DB, src Source, dir Direction, max int) (int, error) {
	// TODO: run migration + record insert in transaction

	migrations, err := Plan(db, src, dir, max)
	if err != nil {
		return 0, err
	}

	// Apply migrations
	applied := 0
	for _, m := range migrations {
		var executor SqlExecutor

		if m.DisableTransaction {
			executor = db
		} else {
			executor, err = db.Begin()
			if err != nil {
				return applied, newTxError(m, err)
			}
		}

		err := func() error {
			for _, stmt := range m.Queries {
				if _, err := executor.Exec(stmt); err != nil {
					return err
				}
			}

			switch dir {
			case Up:
				err = executor.InsertRecord(&Record{
					ID:        m.ID,
					AppliedAt: time.Now(),
				})
				if err != nil {
					return err
				}
			case Down:
				err := executor.DeleteRecord(&Record{
					ID: m.ID,
				})
				if err != nil {
					return err
				}
			default:
				panic(fmt.Sprintf("unexpected direction: %v", dir))
			}

			return nil
		}()

		if tx, ok := executor.(Tx); ok {
			if err != nil {
				tx.Rollback()
				return applied, newTxError(m, err)
			}
			if err := tx.Commit(); err != nil {
				tx.Rollback()
				return applied, newTxError(m, err)
			}
		} else {
			if err != nil {
				return applied, err
			}
		}

		applied++
	}

	return applied, nil
}

// Plan a migration.
func Plan(db DB, src Source, dir Direction, max int) ([]*PlannedMigration, error) {
	migrations, err := src.Find()
	if err != nil {
		return nil, err
	}

	records, err := db.GetRecords()
	if err != nil {
		return nil, err
	}

	// Sort migrations that have been run by ID.
	var existingMigrations []*Migration
	for _, migrationRecord := range records {
		existingMigrations = append(existingMigrations, &Migration{
			ID: migrationRecord.ID,
		})
	}
	sort.Sort(byID(existingMigrations))

	// Make sure all migrations in the database are among the found migrations which
	// are to be applied.
	migrationsSearch := make(map[string]struct{})
	for _, migration := range migrations {
		migrationsSearch[migration.ID] = struct{}{}
	}
	for _, existingMigration := range existingMigrations {
		if _, ok := migrationsSearch[existingMigration.ID]; !ok {
			return nil, newPlanError(existingMigration, "unknown migration in database")
		}
	}

	// Get last migration that was run
	record := &Migration{}
	if len(existingMigrations) > 0 {
		record = existingMigrations[len(existingMigrations)-1]
	}

	result := make([]*PlannedMigration, 0)

	// Add missing migrations up to the last run migration.
	// This can happen for example when merges happened.
	if len(existingMigrations) > 0 {
		result = append(result, toCatchup(migrations, existingMigrations, record)...)
	}

	// Figure out which migrations to apply
	toApply := toApply(migrations, record.ID, dir)
	toApplyCount := len(toApply)
	if max > 0 && max < toApplyCount {
		toApplyCount = max
	}
	for _, v := range toApply[0:toApplyCount] {
		if dir == Up {
			result = append(result, &PlannedMigration{
				Migration:          v,
				Queries:            v.Up,
				DisableTransaction: v.DisableTransactionUp,
			})
		} else if dir == Down {
			result = append(result, &PlannedMigration{
				Migration:          v,
				Queries:            v.Down,
				DisableTransaction: v.DisableTransactionDown,
			})
		}
	}

	return result, nil
}

// Skip a set of migrations
//
// Will skip at most `max` migrations. Pass 0 for no limit.
//
// Returns the number of skipped migrations.
func SkipMax(db DB, src Source, dir Direction, max int) (int, error) {
	migrations, err := Plan(db, src, dir, max)
	if err != nil {
		return 0, err
	}

	// Skip migrations
	applied := 0
	for _, migration := range migrations {
		var executor SqlExecutor

		if migration.DisableTransaction {
			executor = db
		} else {
			executor, err = db.Begin()
			if err != nil {
				return applied, newTxError(migration, err)
			}
		}

		err = executor.InsertRecord(&Record{
			ID:        migration.ID,
			AppliedAt: time.Now(),
		})

		if tx, ok := executor.(Tx); ok {
			if err != nil {
				tx.Rollback()
				return applied, newTxError(migration, err)
			}
			if err := tx.Commit(); err != nil {
				return applied, newTxError(migration, err)
			}
		} else {
			if err != nil {
				return applied, err
			}
		}

		applied++
	}

	return applied, nil
}

// Filter a slice of migrations into ones that should be applied.
func toApply(migrations []*Migration, current string, dir Direction) []*Migration {
	var index = -1
	if current != "" {
		for index < len(migrations)-1 {
			index++
			if migrations[index].ID == current {
				break
			}
		}
	}

	switch dir {
	case Up:
		return migrations[index+1:]
	case Down:
		if index == -1 {
			return []*Migration{}
		}

		// Add in reverse order
		toApply := make([]*Migration, index+1)
		for i := 0; i < index+1; i++ {
			toApply[index-i] = migrations[i]
		}
		return toApply
	}

	panic(fmt.Sprintf("unexpected direction: %v", dir))
}

func toCatchup(migrations, existingMigrations []*Migration, lastRun *Migration) []*PlannedMigration {
	missing := make([]*PlannedMigration, 0)
	for _, migration := range migrations {
		found := false
		for _, existing := range existingMigrations {
			if existing.ID == migration.ID {
				found = true
				break
			}
		}
		if !found && migration.Less(lastRun) {
			missing = append(missing, &PlannedMigration{
				Migration:          migration,
				Queries:            migration.Up,
				DisableTransaction: migration.DisableTransactionUp,
			})
		}
	}
	return missing
}
