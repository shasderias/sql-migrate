package migrate

import (
	"sort"

	. "gopkg.in/check.v1"
)

var toApplyMigrations = []*Migration{
	{ID: "abc", Up: nil, Down: nil},
	{ID: "cde", Up: nil, Down: nil},
	{ID: "efg", Up: nil, Down: nil},
}

type ToApplyMigrateSuite struct {
}

var _ = Suite(&ToApplyMigrateSuite{})

func (s *ToApplyMigrateSuite) TestGetAll(c *C) {
	m := toApply(toApplyMigrations, "", Up)
	c.Assert(m, HasLen, 3)
	c.Assert(m[0], Equals, toApplyMigrations[0])
	c.Assert(m[1], Equals, toApplyMigrations[1])
	c.Assert(m[2], Equals, toApplyMigrations[2])
}

func (s *ToApplyMigrateSuite) TestGetAbc(c *C) {
	m := toApply(toApplyMigrations, "abc", Up)
	c.Assert(m, HasLen, 2)
	c.Assert(m[0], Equals, toApplyMigrations[1])
	c.Assert(m[1], Equals, toApplyMigrations[2])
}

func (s *ToApplyMigrateSuite) TestGetCde(c *C) {
	m := toApply(toApplyMigrations, "cde", Up)
	c.Assert(m, HasLen, 1)
	c.Assert(m[0], Equals, toApplyMigrations[2])
}

func (s *ToApplyMigrateSuite) TestGetDone(c *C) {
	m := toApply(toApplyMigrations, "efg", Up)
	c.Assert(m, HasLen, 0)

	m = toApply(toApplyMigrations, "zzz", Up)
	c.Assert(m, HasLen, 0)
}

func (s *ToApplyMigrateSuite) TestDownDone(c *C) {
	m := toApply(toApplyMigrations, "", Down)
	c.Assert(m, HasLen, 0)
}

func (s *ToApplyMigrateSuite) TestDownCde(c *C) {
	m := toApply(toApplyMigrations, "cde", Down)
	c.Assert(m, HasLen, 2)
	c.Assert(m[0], Equals, toApplyMigrations[1])
	c.Assert(m[1], Equals, toApplyMigrations[0])
}

func (s *ToApplyMigrateSuite) TestDownAbc(c *C) {
	m := toApply(toApplyMigrations, "abc", Down)
	c.Assert(m, HasLen, 1)
	c.Assert(m[0], Equals, toApplyMigrations[0])
}

func (s *ToApplyMigrateSuite) TestDownAll(c *C) {
	m := toApply(toApplyMigrations, "efg", Down)
	c.Assert(m, HasLen, 3)
	c.Assert(m[0], Equals, toApplyMigrations[2])
	c.Assert(m[1], Equals, toApplyMigrations[1])
	c.Assert(m[2], Equals, toApplyMigrations[0])

	m = toApply(toApplyMigrations, "zzz", Down)
	c.Assert(m, HasLen, 3)
	c.Assert(m[0], Equals, toApplyMigrations[2])
	c.Assert(m[1], Equals, toApplyMigrations[1])
	c.Assert(m[2], Equals, toApplyMigrations[0])
}

func (s *ToApplyMigrateSuite) TestAlphaNumericMigrations(c *C) {
	var migrations = byID([]*Migration{
		{ID: "10_abc", Up: nil, Down: nil},
		{ID: "1_abc", Up: nil, Down: nil},
		{ID: "efg", Up: nil, Down: nil},
		{ID: "2_cde", Up: nil, Down: nil},
		{ID: "35_cde", Up: nil, Down: nil},
	})

	sort.Sort(migrations)

	toApplyUp := toApply(migrations, "2_cde", Up)
	c.Assert(toApplyUp, HasLen, 3)
	c.Assert(toApplyUp[0].ID, Equals, "10_abc")
	c.Assert(toApplyUp[1].ID, Equals, "35_cde")
	c.Assert(toApplyUp[2].ID, Equals, "efg")

	toApplyDown := toApply(migrations, "2_cde", Down)
	c.Assert(toApplyDown, HasLen, 2)
	c.Assert(toApplyDown[0].ID, Equals, "2_cde")
	c.Assert(toApplyDown[1].ID, Equals, "1_abc")
}
