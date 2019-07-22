package main

import (
	"flag"

	_ "github.com/lib/pq"
	"github.com/shasderias/sql-migrate/pkg/config"
)

var ConfigFile string
var ConfigEnvironment string

func ConfigFlags(f *flag.FlagSet) {
	f.StringVar(&ConfigFile, "config", "dbconfig.yml", "Configuration file to use.")
	f.StringVar(&ConfigEnvironment, "env", "development", "Environment to use.")
}

func GetEnvironment() (*config.Environment, error) {
	return config.Get(ConfigFile, ConfigEnvironment)
}
