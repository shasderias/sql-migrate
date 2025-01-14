package main

import (
	"fmt"
	"os"

	"github.com/mitchellh/cli"

	_ "github.com/shasderias/sql-migrate/pkg/db/postgres"
)

func main() {
	os.Exit(realMain())
}

var ui cli.Ui

func realMain() int {
	ui = &cli.BasicUi{Writer: os.Stdout}

	cli := &cli.CLI{
		Args: os.Args[1:],
		Commands: map[string]cli.CommandFactory{
			"up": func() (cli.Command, error) {
				return &UpCommand{}, nil
			},
			"down": func() (cli.Command, error) {
				return &DownCommand{}, nil
			},
			"redo": func() (cli.Command, error) {
				return &RedoCommand{}, nil
			},
			"status": func() (cli.Command, error) {
				return &StatusCommand{}, nil
			},
			"new": func() (cli.Command, error) {
				return &NewCommand{}, nil
			},
			"skip": func() (cli.Command, error) {
				return &SkipCommand{}, nil
			},
		},
		HelpFunc: cli.BasicHelpFunc("sql-migrate"),
		Version:  "0.0.4",
	}

	exitCode, err := cli.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error executing CLI: %s\n", err.Error())
		return 1
	}

	return exitCode
}
