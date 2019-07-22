package migrate

import (
	"sort"

	. "gopkg.in/check.v1"
)

type SortSuite struct{}

var _ = Suite(&SortSuite{})

func (s *SortSuite) TestSortMigrations(c *C) {
	var migrations = byID([]*Migration{
		{ID: "10_abc", Up: nil, Down: nil},
		{ID: "120_cde", Up: nil, Down: nil},
		{ID: "1_abc", Up: nil, Down: nil},
		{ID: "efg", Up: nil, Down: nil},
		{ID: "2_cde", Up: nil, Down: nil},
		{ID: "35_cde", Up: nil, Down: nil},
		{ID: "3_efg", Up: nil, Down: nil},
		{ID: "4_abc", Up: nil, Down: nil},
	})

	sort.Sort(migrations)
	c.Assert(migrations, HasLen, 8)
	c.Assert(migrations[0].ID, Equals, "1_abc")
	c.Assert(migrations[1].ID, Equals, "2_cde")
	c.Assert(migrations[2].ID, Equals, "3_efg")
	c.Assert(migrations[3].ID, Equals, "4_abc")
	c.Assert(migrations[4].ID, Equals, "10_abc")
	c.Assert(migrations[5].ID, Equals, "35_cde")
	c.Assert(migrations[6].ID, Equals, "120_cde")
	c.Assert(migrations[7].ID, Equals, "efg")
}
