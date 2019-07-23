package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"

	"github.com/shasderias/sql-migrate/pkg/migrate"
)

type StatusCommand struct {
}

func (c *StatusCommand) Help() string {
	helpText := `
Usage: sql-migrate status [options] ...

  Show migration status.

Options:

  -config=dbconfig.yml   Configuration file to use.
  -env="development"     Environment.

`
	return strings.TrimSpace(helpText)
}

func (c *StatusCommand) Synopsis() string {
	return "Show migration status"
}

func (c *StatusCommand) Run(args []string) int {
	cmdFlags := flag.NewFlagSet("status", flag.ContinueOnError)
	cmdFlags.Usage = func() { ui.Output(c.Help()) }
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

	migrations, err := source.Find()
	if err != nil {
		ui.Error(err.Error())
		return 1
	}

	records, err := migrator.Records()
	if err != nil {
		ui.Error(err.Error())
		return 1
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Migration", "Applied"})
	table.SetColWidth(60)

	rows := make(map[string]*statusRow)

	for _, m := range migrations {
		rows[m.ID] = &statusRow{
			ID:       m.ID,
			Migrated: false,
		}
	}

	for _, r := range records {
		if rows[r.ID] == nil {
			ui.Warn(fmt.Sprintf("Could not find migration file: %v", r.ID))
			continue
		}

		rows[r.ID].Migrated = true
		rows[r.ID].AppliedAt = r.AppliedAt
	}

	for _, m := range migrations {
		if rows[m.ID] != nil && rows[m.ID].Migrated {
			table.Append([]string{
				m.ID,
				rows[m.ID].AppliedAt.String(),
			})
		} else {
			table.Append([]string{
				m.ID,
				"no",
			})
		}
	}

	table.Render()

	return 0
}

type statusRow struct {
	ID        string
	Migrated  bool
	AppliedAt time.Time
}
