package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"
)

const (
	defaultTableName = "migration"
)

type Environment struct {
	Dialect    string `yaml:"dialect"`
	DataSource string `yaml:"datasource"`
	Dir        string `yaml:"dir"`
	TableName  string `yaml:"table"`
}

func Get(filename, envName string) (*Environment, error) {
	envs, err := parseConfigFile(filename)
	if err != nil {
		return nil, err
	}

	env, ok := envs[envName]
	if !ok {
		return nil, fmt.Errorf("no environment named %s", envName)
	}

	if env.Dialect == "" {
		return nil, errors.New("dialect not specified")
	}

	if env.DataSource == "" {
		return nil, errors.New("data source not specified")
	}
	env.DataSource = os.ExpandEnv(env.DataSource)

	if env.Dir == "" {
		env.Dir = "migrations"
	}

	if env.TableName == "" {
		env.TableName = defaultTableName
	}

	return env, nil
}

func parseConfigFile(path string) (map[string]*Environment, error) {
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var envs map[string]*Environment

	err = yaml.Unmarshal(file, &envs)
	if err != nil {
		return nil, err
	}

	return envs, nil
}
