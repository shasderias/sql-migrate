package migrate_test

import (
	. "gopkg.in/check.v1"

	_ "github.com/shasderias/sql-migrate/pkg/db/sqlite"
	"github.com/shasderias/sql-migrate/pkg/migrate"
)

var sqliteMigrations = []*migrate.Migration{
	{
		ID:   "123",
		Up:   []string{"CREATE TABLE people (id int)"},
		Down: []string{"DROP TABLE people"},
	},
	{
		ID:   "124",
		Up:   []string{"ALTER TABLE people ADD COLUMN first_name text"},
		Down: []string{"SELECT 0"}, // Not really supported
	},
}

type SqliteMigrateSuite struct {
	migrate.DB
}

var _ = Suite(&SqliteMigrateSuite{})

func (s *SqliteMigrateSuite) SetUpTest(c *C) {
	db, err := migrate.GetDB("sqlite3", ":memory:", "migration")
	c.Assert(err, IsNil)
	c.Assert(db, Not(IsNil))

	s.DB = db
}

func (s *SqliteMigrateSuite) TestRunMigration(c *C) {
	migrations := &migrate.MemorySource{
		Migrations: sqliteMigrations[:1],
	}

	// Executes one migration
	n, err := migrate.Exec(s, migrations, migrate.Up)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)

	// Can use table now
	_, err = s.Exec("SELECT * FROM people")
	c.Assert(err, IsNil)

	// Shouldn't apply migration again
	n, err = migrate.Exec(s.DB, migrations, migrate.Up)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 0)
}

func (s *SqliteMigrateSuite) TestRunMigrationEscapeTable(c *C) {
	migrations := &migrate.MemorySource{
		Migrations: sqliteMigrations[:1],
	}

	db, err := migrate.GetDB("sqlite3", ":memory:", "my migrations")
	c.Assert(err, IsNil)

	// Executes one migration
	n, err := migrate.Exec(db, migrations, migrate.Up)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)
}

func (s *SqliteMigrateSuite) TestMigrateMultiple(c *C) {
	migrations := &migrate.MemorySource{
		Migrations: sqliteMigrations[:2],
	}

	// Executes two migrations
	n, err := migrate.Exec(s, migrations, migrate.Up)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 2)

	// Can use column now
	_, err = s.Exec("SELECT first_name FROM people")
	c.Assert(err, IsNil)
}

func (s *SqliteMigrateSuite) TestMigrateIncremental(c *C) {
	migrations := &migrate.MemorySource{
		Migrations: sqliteMigrations[:1],
	}

	// Executes one migration
	n, err := migrate.Exec(s, migrations, migrate.Up)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)

	// Execute a new migration
	migrations = &migrate.MemorySource{
		Migrations: sqliteMigrations[:2],
	}
	n, err = migrate.Exec(s, migrations, migrate.Up)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)

	// Can use column now
	_, err = s.Exec("SELECT first_name FROM people")
	c.Assert(err, IsNil)
}

func (s *SqliteMigrateSuite) TestFileMigrate(c *C) {
	migrations := &migrate.FileSource{
		Dir: "test-migrations",
	}

	// Executes two migrations
	n, err := migrate.Exec(s, migrations, migrate.Up)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 2)

	// Has data
	var id int
	row := s.SqlDB().QueryRow("SELECT id FROM people")
	err = row.Scan(&id)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int(1))
}

//func (s *SqliteMigrateSuite) TestHttpFileSystemMigrate(c *C) {
//	migrations := &migrate.HttpFileSystemMigrationSource{
//		FileSystem: http.Dir("test-migrations),
//	}
//
//	// Executes two migrations
//	n, err := migrate.Exec(s, migrations, migrate.Up)
//	c.Assert(err, IsNil)
//	c.Assert(n, Equals, 2)
//
//	// Has data
//	id, err := s.SelectInt("SELECT id FROM people")
//	c.Assert(err, IsNil)
//	c.Assert(id, Equals, int64(1))
//}

//func (s *SqliteMigrateSuite) TestAssetMigrate(c *C) {
//	migrations := &AssetMigrationSource{
//		Asset:    Asset,
//		AssetDir: AssetDir,
//		Dir:      "test-migrations,
//	}
//
//	// Executes two migrations
//	n, err := migrate.Exec(s, migrations, migrate.Up)
//	c.Assert(err, IsNil)
//	c.Assert(n, Equals, 2)
//
//	// Has data
//	id, err := s.SelectInt("SELECT id FROM people")
//	c.Assert(err, IsNil)
//	c.Assert(id, Equals, int64(1))
//}

func (s *SqliteMigrateSuite) TestMigrateMax(c *C) {
	migrations := &migrate.FileSource{
		Dir: "test-migrations",
	}

	// Executes one migration
	n, err := migrate.ExecMax(s, migrations, migrate.Up, 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)

	var id int
	row := s.SqlDB().QueryRow("SELECT COUNT(*) FROM people")
	err = row.Scan(&id)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int(0))
}

func (s *SqliteMigrateSuite) TestMigrateDown(c *C) {
	migrations := &migrate.FileSource{
		Dir: "test-migrations",
	}

	n, err := migrate.Exec(s, migrations, migrate.Up)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 2)

	// Has data
	var id int
	row := s.SqlDB().QueryRow("SELECT id FROM people")
	err = row.Scan(&id)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int(1))

	// Undo the last one
	n, err = migrate.ExecMax(s, migrations, migrate.Down, 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)

	// No more data
	row = s.SqlDB().QueryRow("SELECT COUNT(*) FROM people")
	err = row.Scan(&id)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int(0))

	// Remove the table.
	n, err = migrate.ExecMax(s, migrations, migrate.Down, 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)

	// Cannot query it anymore
	row = s.SqlDB().QueryRow("SELECT COUNT(*) FROM people")
	err = row.Scan(&id)
	c.Assert(err, Not(IsNil))

	// Nothing left to do.
	n, err = migrate.ExecMax(s, migrations, migrate.Down, 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 0)
}

func (s *SqliteMigrateSuite) TestMigrateDownFull(c *C) {
	migrations := &migrate.FileSource{
		Dir: "test-migrations",
	}

	n, err := migrate.Exec(s, migrations, migrate.Up)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 2)

	// Has data
	var id int
	row := s.SqlDB().QueryRow("SELECT id FROM people")
	err = row.Scan(&id)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, int(1))

	// Undo the last one
	n, err = migrate.Exec(s, migrations, migrate.Down)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 2)

	// Cannot query it anymore
	row = s.SqlDB().QueryRow("SELECT COUNT(*) FROM people")
	err = row.Scan(&id)
	c.Assert(err, Not(IsNil))

	// Nothing left to do.
	n, err = migrate.Exec(s, migrations, migrate.Down)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 0)
}

func (s *SqliteMigrateSuite) TestMigrateTransaction(c *C) {
	migrations := &migrate.MemorySource{
		Migrations: []*migrate.Migration{
			sqliteMigrations[0],
			sqliteMigrations[1],
			{
				ID:   "125",
				Up:   []string{"INSERT INTO people (id, first_name) VALUES (1, 'Test')", "SELECT fail"},
				Down: []string{}, // Not important here
			},
		},
	}

	// Should fail, transaction should roll back the INSERT.
	n, err := migrate.Exec(s, migrations, migrate.Up)
	c.Assert(err, Not(IsNil))
	c.Assert(n, Equals, 2)

	// INSERT should be rolled back
	var count int
	row := s.SqlDB().QueryRow("SELECT COUNT(*) FROM people")
	err = row.Scan(&count)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int(0))
}

func (s *SqliteMigrateSuite) TestPlanMigration(c *C) {
	migrations := &migrate.MemorySource{
		Migrations: []*migrate.Migration{
			{
				ID:   "1_create_table.sql",
				Up:   []string{"CREATE TABLE people (id int)"},
				Down: []string{"DROP TABLE people"},
			},
			{
				ID:   "2_alter_table.sql",
				Up:   []string{"ALTER TABLE people ADD COLUMN first_name text"},
				Down: []string{"SELECT 0"}, // Not really supported
			},
			{
				ID:   "10_add_last_name.sql",
				Up:   []string{"ALTER TABLE people ADD COLUMN last_name text"},
				Down: []string{"ALTER TABLE people DROP COLUMN last_name"},
			},
		},
	}
	n, err := migrate.Exec(s, migrations, migrate.Up)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)

	migrations.Migrations = append(migrations.Migrations, &migrate.Migration{
		ID:   "11_add_middle_name.sql",
		Up:   []string{"ALTER TABLE people ADD COLUMN middle_name text"},
		Down: []string{"ALTER TABLE people DROP COLUMN middle_name"},
	})

	plannedMigrations, err := migrate.Plan(s, migrations, migrate.Up, 0)
	c.Assert(err, IsNil)
	c.Assert(plannedMigrations, HasLen, 1)
	c.Assert(plannedMigrations[0].Migration, Equals, migrations.Migrations[3])

	plannedMigrations, err = migrate.Plan(s, migrations, migrate.Down, 0)
	c.Assert(err, IsNil)
	c.Assert(plannedMigrations, HasLen, 3)
	c.Assert(plannedMigrations[0].Migration, Equals, migrations.Migrations[2])
	c.Assert(plannedMigrations[1].Migration, Equals, migrations.Migrations[1])
	c.Assert(plannedMigrations[2].Migration, Equals, migrations.Migrations[0])
}

func (s *SqliteMigrateSuite) TestSkipMigration(c *C) {
	migrations := &migrate.MemorySource{
		Migrations: []*migrate.Migration{
			{
				ID:   "1_create_table.sql",
				Up:   []string{"CREATE TABLE people (id int)"},
				Down: []string{"DROP TABLE people"},
			},
			{
				ID:   "2_alter_table.sql",
				Up:   []string{"ALTER TABLE people ADD COLUMN first_name text"},
				Down: []string{"SELECT 0"}, // Not really supported
			},
			{
				ID:   "10_add_last_name.sql",
				Up:   []string{"ALTER TABLE people ADD COLUMN last_name text"},
				Down: []string{"ALTER TABLE people DROP COLUMN last_name"},
			},
		},
	}
	n, err := migrate.SkipMax(s, migrations, migrate.Up, 0)
	// there should be no errors
	c.Assert(err, IsNil)
	// we should have detected and skipped 3 migrations
	c.Assert(n, Equals, 3)
	// should not actually have the tables now since it was skipped
	// so this query should fail
	_, err = s.Exec("SELECT * FROM people")
	c.Assert(err, NotNil)
	// run the migrations again, should execute none of them since we pegged the db level
	// in the skip command
	n2, err2 := migrate.Exec(s, migrations, migrate.Up)
	// there should be no errors
	c.Assert(err2, IsNil)
	// we should not have executed any migrations
	c.Assert(n2, Equals, 0)
}

func (s *SqliteMigrateSuite) TestPlanMigrationWithHoles(c *C) {
	up := "SELECT 0"
	down := "SELECT 1"
	migrations := &migrate.MemorySource{
		Migrations: []*migrate.Migration{
			{
				ID:   "1",
				Up:   []string{up},
				Down: []string{down},
			},
			{
				ID:   "3",
				Up:   []string{up},
				Down: []string{down},
			},
		},
	}
	n, err := migrate.Exec(s, migrations, migrate.Up)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 2)

	migrations.Migrations = append(migrations.Migrations, &migrate.Migration{
		ID:   "2",
		Up:   []string{up},
		Down: []string{down},
	})

	migrations.Migrations = append(migrations.Migrations, &migrate.Migration{
		ID:   "4",
		Up:   []string{up},
		Down: []string{down},
	})

	migrations.Migrations = append(migrations.Migrations, &migrate.Migration{
		ID:   "5",
		Up:   []string{up},
		Down: []string{down},
	})

	// apply all the missing migrations
	plannedMigrations, err := migrate.Plan(s, migrations, migrate.Up, 0)
	c.Assert(err, IsNil)
	c.Assert(plannedMigrations, HasLen, 3)
	c.Assert(plannedMigrations[0].Migration.ID, Equals, "2")
	c.Assert(plannedMigrations[0].Queries[0], Equals, up)
	c.Assert(plannedMigrations[1].Migration.ID, Equals, "4")
	c.Assert(plannedMigrations[1].Queries[0], Equals, up)
	c.Assert(plannedMigrations[2].Migration.ID, Equals, "5")
	c.Assert(plannedMigrations[2].Queries[0], Equals, up)

	// first catch up to current target state 123, then migrate down 1 step to 12
	plannedMigrations, err = migrate.Plan(s, migrations, migrate.Down, 1)
	c.Assert(err, IsNil)
	c.Assert(plannedMigrations, HasLen, 2)
	c.Assert(plannedMigrations[0].Migration.ID, Equals, "2")
	c.Assert(plannedMigrations[0].Queries[0], Equals, up)
	c.Assert(plannedMigrations[1].Migration.ID, Equals, "3")
	c.Assert(plannedMigrations[1].Queries[0], Equals, down)

	// first catch up to current target state 123, then migrate down 2 steps to 1
	plannedMigrations, err = migrate.Plan(s, migrations, migrate.Down, 2)
	c.Assert(err, IsNil)
	c.Assert(plannedMigrations, HasLen, 3)
	c.Assert(plannedMigrations[0].Migration.ID, Equals, "2")
	c.Assert(plannedMigrations[0].Queries[0], Equals, up)
	c.Assert(plannedMigrations[1].Migration.ID, Equals, "3")
	c.Assert(plannedMigrations[1].Queries[0], Equals, down)
	c.Assert(plannedMigrations[2].Migration.ID, Equals, "2")
	c.Assert(plannedMigrations[2].Queries[0], Equals, down)
}

func (s *SqliteMigrateSuite) TestLess(c *C) {
	c.Assert((migrate.Migration{ID: "1"}).Less(&migrate.Migration{ID: "2"}), Equals, true)           // 1 less than 2
	c.Assert((migrate.Migration{ID: "2"}).Less(&migrate.Migration{ID: "1"}), Equals, false)          // 2 not less than 1
	c.Assert((migrate.Migration{ID: "1"}).Less(&migrate.Migration{ID: "a"}), Equals, true)           // 1 less than a
	c.Assert((migrate.Migration{ID: "a"}).Less(&migrate.Migration{ID: "1"}), Equals, false)          // a not less than 1
	c.Assert((migrate.Migration{ID: "a"}).Less(&migrate.Migration{ID: "a"}), Equals, false)          // a not less than a
	c.Assert((migrate.Migration{ID: "1-a"}).Less(&migrate.Migration{ID: "1-b"}), Equals, true)       // 1-a less than 1-b
	c.Assert((migrate.Migration{ID: "1-b"}).Less(&migrate.Migration{ID: "1-a"}), Equals, false)      // 1-b not less than 1-a
	c.Assert((migrate.Migration{ID: "1"}).Less(&migrate.Migration{ID: "10"}), Equals, true)          // 1 less than 10
	c.Assert((migrate.Migration{ID: "10"}).Less(&migrate.Migration{ID: "1"}), Equals, false)         // 10 not less than 1
	c.Assert((migrate.Migration{ID: "1_foo"}).Less(&migrate.Migration{ID: "10_bar"}), Equals, true)  // 1_foo not less than 1
	c.Assert((migrate.Migration{ID: "10_bar"}).Less(&migrate.Migration{ID: "1_foo"}), Equals, false) // 10 not less than 1
	// 20160126_1100 less than 20160126_1200
	c.Assert((migrate.Migration{ID: "20160126_1100"}).
		Less(&migrate.Migration{ID: "20160126_1200"}), Equals, true)
	// 20160126_1200 not less than 20160126_1100
	c.Assert((migrate.Migration{ID: "20160126_1200"}).
		Less(&migrate.Migration{ID: "20160126_1100"}), Equals, false)

}

func (s *SqliteMigrateSuite) TestPlanMigrationWithUnknownDatabaseMigrationApplied(c *C) {
	migrations := &migrate.MemorySource{
		Migrations: []*migrate.Migration{
			{
				ID:   "1_create_table.sql",
				Up:   []string{"CREATE TABLE people (id int)"},
				Down: []string{"DROP TABLE people"},
			},
			{
				ID:   "2_alter_table.sql",
				Up:   []string{"ALTER TABLE people ADD COLUMN first_name text"},
				Down: []string{"SELECT 0"}, // Not really supported
			},
			{
				ID:   "10_add_last_name.sql",
				Up:   []string{"ALTER TABLE people ADD COLUMN last_name text"},
				Down: []string{"ALTER TABLE people DROP COLUMN last_name"},
			},
		},
	}
	n, err := migrate.Exec(s, migrations, migrate.Up)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)

	// Note that migration 10_add_last_name.sql is missing from the new migrations source
	// so it is considered an "unknown" migration for the planner.
	migrations.Migrations = append(migrations.Migrations[:2], &migrate.Migration{
		ID:   "10_add_middle_name.sql",
		Up:   []string{"ALTER TABLE people ADD COLUMN middle_name text"},
		Down: []string{"ALTER TABLE people DROP COLUMN middle_name"},
	})

	_, err = migrate.Plan(s, migrations, migrate.Up, 0)
	c.Assert(err, NotNil, Commentf("migrate.Up migrations should not have been applied when there "+
		"is an unknown migration in the database"))
	c.Assert(err, FitsTypeOf, &migrate.PlanError{})

	_, err = migrate.Plan(s, migrations, migrate.Down, 0)
	c.Assert(err, NotNil, Commentf("Down migrations should not have been applied when there "+
		"is an unknown migration in the database"))
	c.Assert(err, FitsTypeOf, &migrate.PlanError{})
}

// TestExecWithUnknownMigrationInDatabase makes sure that problems found with planning the
// migrations are propagated and returned by Exec.
func (s *SqliteMigrateSuite) TestExecWithUnknownMigrationInDatabase(c *C) {
	migrations := &migrate.MemorySource{
		Migrations: sqliteMigrations[:2],
	}

	// Executes two migrations
	n, err := migrate.Exec(s, migrations, migrate.Up)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 2)

	// Then create a new migration source with one of the migrations missing
	var newSqliteMigrations = []*migrate.Migration{
		{
			ID:   "124_other",
			Up:   []string{"ALTER TABLE people ADD COLUMN middle_name text"},
			Down: []string{"ALTER TABLE people DROP COLUMN middle_name"},
		},
		{
			ID:   "125",
			Up:   []string{"ALTER TABLE people ADD COLUMN age int"},
			Down: []string{"ALTER TABLE people DROP COLUMN age"},
		},
	}
	migrations = &migrate.MemorySource{
		Migrations: append(sqliteMigrations[:1], newSqliteMigrations...),
	}

	n, err = migrate.Exec(s, migrations, migrate.Up)
	c.Assert(err, NotNil, Commentf("Migrations should not have been applied when there "+
		"is an unknown migration in the database"))
	c.Assert(err, FitsTypeOf, &migrate.PlanError{})
	c.Assert(n, Equals, 0)

	// Make sure the new columns are not actually created
	_, err = s.Exec("SELECT middle_name FROM people")
	c.Assert(err, NotNil)
	_, err = s.Exec("SELECT age FROM people")
	c.Assert(err, NotNil)
}
