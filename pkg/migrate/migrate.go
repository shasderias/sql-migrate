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

type Migrator struct {
	DB
}

func New(dialect, datasource, tableName string) (*Migrator, error) {
	db, err := getDB(dialect, datasource, tableName)
	if err != nil {
		return nil, err
	}

	return &Migrator{
		DB: db,
	}, nil
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
func (m *Migrator) Exec(src Source, dir Direction) (int, error) {
	return m.ExecMax(src, dir, 0)
}

// ExecMax executes a set of migrations, up to a maximum of max migrations, and
// returns the number of applied migrations.
//
// Pass 0 for no limit (or use Exec).
func (m *Migrator) ExecMax(src Source, dir Direction, max int) (int, error) {
	// TODO: run migration + record insert in transaction

	migrations, err := m.Plan(src, dir, max)
	if err != nil {
		return 0, err
	}

	// Apply migrations
	applied := 0
	for _, mig := range migrations {
		var executor SqlExecutor

		if mig.DisableTransaction {
			executor = m.DB
		} else {
			executor, err = m.DB.Begin()
			if err != nil {
				return applied, newTxError(mig, err)
			}
		}

		err := func() error {
			for _, stmt := range mig.Queries {
				if _, err := executor.Exec(stmt); err != nil {
					return err
				}
			}

			switch dir {
			case Up:
				err = executor.InsertRecord(&Record{
					ID:        mig.ID,
					AppliedAt: time.Now(),
				})
				if err != nil {
					return err
				}
			case Down:
				err := executor.DeleteRecord(&Record{
					ID: mig.ID,
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
				return applied, newTxError(mig, err)
			}
			if err := tx.Commit(); err != nil {
				tx.Rollback()
				return applied, newTxError(mig, err)
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
func (m *Migrator) Plan(src Source, dir Direction, max int) ([]*PlannedMigration, error) {
	migrations, err := src.Find()
	if err != nil {
		return nil, err
	}

	records, err := m.DB.Records()
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
func (m *Migrator) SkipMax(src Source, dir Direction, max int) (int, error) {
	migrations, err := m.Plan(src, dir, max)
	if err != nil {
		return 0, err
	}

	// Skip migrations
	applied := 0
	for _, migration := range migrations {
		var executor SqlExecutor

		if migration.DisableTransaction {
			executor = m.DB
		} else {
			executor, err = m.DB.Begin()
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
