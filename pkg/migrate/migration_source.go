package migrate

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
)

type Source interface {
	// Finds the migrations.
	//
	// The resulting slice of migrations must be sorted by ID.
	Find() ([]*Migration, error)
}

// A hardcoded set of migrations, in-memory.
type MemorySource struct {
	Migrations []*Migration
}

var _ Source = (*MemorySource)(nil)

func (m MemorySource) Find() ([]*Migration, error) {
	// Make sure migrations are sorted. In order to make the MemorySource safe for
	// concurrent use we should not mutate it in place. So `Find` would sort a copy
	// of the m.Migrations.
	migrations := make([]*Migration, len(m.Migrations))
	copy(migrations, m.Migrations)
	sort.Sort(byID(migrations))
	return migrations, nil
}

// A set of migrations loaded from a directory.
type FileSource struct {
	Dir string
}

var _ Source = (*FileSource)(nil)

func (f FileSource) Find() ([]*Migration, error) {
	filesystem := http.Dir(f.Dir)
	return findMigrations(filesystem)
}

func findMigrations(dir http.FileSystem) ([]*Migration, error) {
	migrations := make([]*Migration, 0)

	file, err := dir.Open("/")
	if err != nil {
		return nil, err
	}

	files, err := file.Readdir(0)
	if err != nil {
		return nil, err
	}

	for _, info := range files {
		if strings.HasSuffix(info.Name(), ".sql") {
			file, err := dir.Open(info.Name())
			if err != nil {
				return nil, fmt.Errorf("Error while opening %s: %s", info.Name(), err)
			}

			migration, err := parse(info.Name(), file)
			if err != nil {
				return nil, fmt.Errorf("Error while parsing %s: %s", info.Name(), err)
			}

			migrations = append(migrations, migration)
		}
	}

	// Make sure migrations are sorted
	sort.Sort(byID(migrations))

	return migrations, nil
}
