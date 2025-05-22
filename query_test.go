package pgd

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestColumnSelector(t *testing.T) {

	cs := ColumnSelector("a.x.b.y.c.z")
	Convey("Given column selector "+cs.String(), t, func() {
		Convey("except last column", func() {
			prefix, c := cs.SplitAtLastColumn()
			So(prefix, ShouldEqual, "a.x.b.y.c")
			So(c, ShouldEqual, "z")
		})

		Convey("breakdown", func() {
			tables, cols := cs.Breakdown()
			So(tables, ShouldEqual, []Table{"a", "b", "c"})
			So(cols, ShouldEqual, []Column{"x", "y", "z"})

			Convey("reconstruct", func() {
				So(ColumnSelectorRebuild(tables, cols), ShouldEqual, cs)
			})

			Convey("reconstruct up to 2nd table", func() {
				So(ColumnSelectorRebuild(tables[:2], cols[:2]), ShouldEqual, ColumnSelector("a.x.b.y"))
			})
		})
	})
}

func TestConvertQuery(t *testing.T) {

	tables := TablesMetadata{
		"table1": {
			Name: "table1",
			Columns: map[Column]ColumnMetadata{
				"id":   {Name: "id", DataType: "integer"},
				"name": {Name: "name", DataType: "text"},
				"age":  {Name: "age", DataType: "integer"},
				"other": {Name: "other", DataType: "integer",
					IsNullable: false,
					Relation:   &ColumnRelation{Table: "table2", Column: "id"}},
				"other_null": {Name: "other_null", DataType: "integer",
					IsNullable: true,
					Relation:   &ColumnRelation{Table: "table2", Column: "id"}},
			},
		},
		"table2": { // foreign table
			Name: "table2",
			Columns: map[Column]ColumnMetadata{
				"id":   {Name: "id", DataType: "integer", IsNullable: false},
				"name": {Name: "name", DataType: "text"},
			},
		},
	}

	tcs := []struct {
		name               string
		query              Query
		expectedQuery      string
		expectedArgs       []any
		expectedTotalQuery string
		expectedTotalArgs  []any
	}{
		{
			name: "simple select",
			query: Query{
				Select: []ColumnSelector{
					"table1.id",
					"table1.name",
					"table1.age",
				},
				Limit: 10,
			},
			expectedQuery:      `SELECT "table1"."id", "table1"."name", "table1"."age" FROM "table1" LIMIT 10 OFFSET 0`,
			expectedTotalQuery: `SELECT count(*) FROM "table1"`,
		},
		{
			name: "select, where",
			query: Query{
				Select: []ColumnSelector{
					"table1.id",
					"table1.name",
					"table1.age",
				},
				Where: &WhereExpression{
					Filter: &Filter{
						Column: "table1.name",
						Op:     "equal",
						Value:  "John Doe",
					},
				},
				Limit: 10,
			},
			expectedQuery:      `SELECT "table1"."id", "table1"."name", "table1"."age" FROM "table1" WHERE "table1"."name" = $1 LIMIT 10 OFFSET 0`,
			expectedArgs:       []any{"John Doe"},
			expectedTotalQuery: `SELECT count(*) FROM "table1" WHERE "table1"."name" = $1`,
			expectedTotalArgs:  []any{"John Doe"},
		},
		{
			name: "select, orderby",
			query: Query{
				Select: []ColumnSelector{
					"table1.id",
					"table1.name",
					"table1.age",
				},
				OrderBy: []OrderByExpression{{ColumnSelector: "table1.name"}},
				Limit:   10,
			},
			expectedQuery:      `SELECT "table1"."id", "table1"."name", "table1"."age" FROM "table1" ORDER BY "table1"."name" LIMIT 10 OFFSET 0`,
			expectedTotalQuery: `SELECT count(*) FROM "table1"`,
		},
		{
			name: "select, orderby desc",
			query: Query{
				Select: []ColumnSelector{
					"table1.id",
					"table1.name",
					"table1.age",
				},
				OrderBy: []OrderByExpression{{ColumnSelector: "table1.name", Descending: true}},
				Limit:   10,
			},
			expectedQuery:      `SELECT "table1"."id", "table1"."name", "table1"."age" FROM "table1" ORDER BY "table1"."name" DESC LIMIT 10 OFFSET 0`,
			expectedTotalQuery: `SELECT count(*) FROM "table1"`,
		},
		{
			name: "select, orderby multiple",
			query: Query{
				Select: []ColumnSelector{
					"table1.id",
					"table1.name",
					"table1.age",
				},
				OrderBy: []OrderByExpression{
					{ColumnSelector: "table1.name", Descending: true},
					{ColumnSelector: "table1.age"}},
				Limit: 10,
			},
			expectedQuery:      `SELECT "table1"."id", "table1"."name", "table1"."age" FROM "table1" ORDER BY "table1"."name" DESC, "table1"."age" LIMIT 10 OFFSET 0`,
			expectedTotalQuery: `SELECT count(*) FROM "table1"`,
		},
		{
			name: "select, where with and conjunction",
			query: Query{
				Select: []ColumnSelector{
					"table1.id",
					"table1.name",
					"table1.age",
				},
				Where: &WhereExpression{
					And: []WhereExpression{
						{
							Filter: &Filter{
								Column: ColumnSelector("table1.name"),
								Op:     "equal",
								Value:  "John Doe",
							}},
						{
							Filter: &Filter{
								Column: ColumnSelector("table1.age"),
								Op:     "greater",
								Value:  30,
							}},
					}},
				Limit: 10,
			},
			expectedQuery:      `SELECT "table1"."id", "table1"."name", "table1"."age" FROM "table1" WHERE ("table1"."name" = $1 AND "table1"."age" > $2) LIMIT 10 OFFSET 0`,
			expectedArgs:       []any{"John Doe", 30},
			expectedTotalQuery: `SELECT count(*) FROM "table1" WHERE ("table1"."name" = $1 AND "table1"."age" > $2)`,
			expectedTotalArgs:  []any{"John Doe", 30},
		},
		{
			name: "select with foreign relation (not null)",
			query: Query{
				Select: []ColumnSelector{
					"table1.id",
					"table1.name",
					"table1.other.table2.id"},
				Limit: 5,
			},
			expectedQuery:      `SELECT "table1"."id", "table1"."name", "table1.other.table2"."id" FROM "table1" INNER JOIN "table2" AS "table1.other.table2" ON "table1"."other" = "table1.other.table2"."id" LIMIT 5 OFFSET 0`,
			expectedTotalQuery: `SELECT count(*) FROM "table1" INNER JOIN "table2" AS "table1.other.table2" ON "table1"."other" = "table1.other.table2"."id"`,
		},
		{
			name: "select with foreign relation (null)",
			query: Query{
				Select: []ColumnSelector{
					"table1.id",
					"table1.name",
					"table1.other_null.table2.id"},
				Limit: 5,
			},
			expectedQuery:      `SELECT "table1"."id", "table1"."name", "table1.other_null.table2"."id" FROM "table1" LEFT JOIN "table2" AS "table1.other_null.table2" ON "table1"."other_null" = "table1.other_null.table2"."id" LIMIT 5 OFFSET 0`,
			expectedTotalQuery: `SELECT count(*) FROM "table1" LEFT JOIN "table2" AS "table1.other_null.table2" ON "table1"."other_null" = "table1.other_null.table2"."id"`,
		},
	}

	api, err := NewAPI(Config{})
	if err != nil {
		t.Fatalf("Failed to create API: %v", err)
	}

	Convey("Given test cases", t, func() {
		Convey("tables should be valid", func() {
			So(tables.Validate(), ShouldBeNil)
		})

		for idx, tc := range tcs {
			Convey(fmt.Sprintf("index %d, %s", idx, tc.name), func() {

				Convey("query should be valid", func() {
					So(tc.query.Validate(), ShouldBeNil)

					// Call the function to be tested
					qPage, qTotal, err := api.convertQuery(tables, tc.query)
					So(err, ShouldBeNil)

					Convey("convert page query to sql", func() {
						q, args, err := qPage.ToSql()
						So(err, ShouldBeNil)

						Convey("query string should match expected", func() {
							So(q, ShouldEqual, tc.expectedQuery)
						})
						Convey("query args should match expected", func() {
							So(args, ShouldResemble, tc.expectedArgs)
						})
					})

					Convey("convert total query to sql", func() {
						q, args, err := qTotal.ToSql()
						So(err, ShouldBeNil)

						Convey("query string should match expected", func() {
							So(q, ShouldEqual, tc.expectedTotalQuery)
						})
						Convey("query args should match expected", func() {
							So(args, ShouldResemble, tc.expectedTotalArgs)
						})
					})
				})
			})
		}
	})
}
