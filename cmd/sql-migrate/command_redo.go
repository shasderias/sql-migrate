package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/shasderias/sql-migrate/pkg/migrate"
)

type RedoCommand struct {
}

func (c *RedoCommand) Help() string {
	helpText := `
Usage: sql-migrate redo [options] ...

  Reapply the last migration.

Options:

  -config=dbconfig.yml   Configuration file to use.
  -env="development"     Environment.
  -dryrun                Don't apply migrations, just print them.

`
	return strings.TrimSpace(helpText)
}

func (c *RedoCommand) Synopsis() string {
	return "Reapply the last migration"
}

func (c *RedoCommand) Run(args []string) int {
	var dryrun bool

	cmdFlags := flag.NewFlagSet("redo", flag.ContinueOnError)
	cmdFlags.Usage = func() { ui.Output(c.Help()) }
	cmdFlags.BoolVar(&dryrun, "dryrun", false, "Don't apply migrations, just print them.")
	ConfigFlags(cmdFlags)

	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	env, err := GetEnvironment()
	if err != nil {
		ui.Error(fmt.Sprintf("Could not parse config: %s", err))
		return 1
	}

	migrator, err := migrate.New(env.Dialect, env.DataSource, env.TableName)
	if err != nil {
		ui.Error(err.Error())
		return 1
	}

	source := migrate.FileSource{
		Dir: env.Dir,
	}

	migrations, err := migrator.Plan(source, migrate.Down, 1)
	if len(migrations) == 0 {
		ui.Output("Nothing to do!")
		return 0
	}

	if dryrun {
		PrintMigration(migrations[0], migrate.Down)
		PrintMigration(migrations[0], migrate.Up)
	} else {
		_, err := migrator.ExecMax(source, migrate.Down, 1)
		if err != nil {
			ui.Error(fmt.Sprintf("Migration (down) failed: %s", err))
			return 1
		}

		_, err = migrator.ExecMax(source, migrate.Up, 1)
		if err != nil {
			ui.Error(fmt.Sprintf("Migration (up) failed: %s", err))
			return 1
		}

		ui.Output(fmt.Sprintf("Reapplied migration %s.", migrations[0].ID))
	}

	return 0
}
