package main

import (
	"fmt"

	"github.com/shasderias/sql-migrate/pkg/migrate"
)

func ApplyMigrations(dir migrate.Direction, dryrun bool, limit int) error {
	env, err := GetEnvironment()
	if err != nil {
		return fmt.Errorf("error parsing config: %s", err)
	}

	db, err := migrate.GetDB(env.Dialect, env.DataSource, env.TableName)
	if err != nil {
		return err
	}

	source := migrate.FileSource{
		Dir: env.Dir,
	}

	if dryrun {
		migrations, err := migrate.Plan(db, source, dir, limit)
		if err != nil {
			return fmt.Errorf("error planning migration: %s", err)
		}

		for _, m := range migrations {
			PrintMigration(m, dir)
		}
	} else {
		n, err := migrate.ExecMax(db, source, dir, limit)
		if err != nil {
			return fmt.Errorf("migration failed: %s", err)
		}

		if n == 1 {
			ui.Output("Applied 1 migration")
		} else {
			ui.Output(fmt.Sprintf("Applied %d migrations", n))
		}
	}

	return nil
}

func PrintMigration(m *migrate.PlannedMigration, dir migrate.Direction) {
	switch dir {
	case migrate.Up:
		ui.Output(fmt.Sprintf("==> Will apply migration %s (up)", m.ID))
		for _, q := range m.Up {
			ui.Output(q)
		}
	case migrate.Down:
		ui.Output(fmt.Sprintf("==> Will apply migration %s (down)", m.ID))
		for _, q := range m.Down {
			ui.Output(q)
		}
	}
	panic("unreachable code reached")
}
