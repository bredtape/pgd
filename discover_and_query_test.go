package pgd

import (
	"context"
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

// uses table names: tableA, tableB, tableC

func TestDiscoverAndQueryData(t *testing.T) {
	ctx := context.Background()

	tcs := []struct {
		Desc     string
		Query    Query
		Expected QueryResult
	}{
		{
			Desc: "Select all columns from tableA",
			Query: Query{
				Select: []ColumnSelector{
					"tableA.id",
					"tableA.name",
					"tableA.age",
					"tableA.other_b",
					"tableA.other_b2",
				},
				Limit: 5},
			Expected: QueryResult{
				Data: []map[string]any{
					{"tableA.id": int32(4), "tableA.name": "Alice", "tableA.age": 30.0, "tableA.other_b": int32(1), "tableA.other_b2": int32(2)},
					{"tableA.id": int32(5), "tableA.name": "Bob", "tableA.age": 25.0, "tableA.other_b": int32(2), "tableA.other_b2": nil},
					{"tableA.id": int32(6), "tableA.name": "Charlie", "tableA.age": 35.0, "tableA.other_b": int32(2), "tableA.other_b2": int32(3)},
				},
				Limit: 5,
				Total: 3,
			},
		},
		{
			Desc: "select columns from a, simple filter",
			Query: Query{
				Select: []ColumnSelector{
					"tableA.id",
					"tableA.name",
				},
				Where: &WhereExpression{
					Filter: &Filter{
						Column:   "tableA.name",
						Operator: "equal",
						Value:    "Bob"},
				},
				Limit: 5},
			Expected: QueryResult{
				Data: []map[string]any{
					{"tableA.id": int32(5), "tableA.name": "Bob"},
				},
				Limit: 5,
				Total: 1,
			},
		},
		{
			Desc: "select some columns from a and b",
			Query: Query{
				Select: []ColumnSelector{
					"tableA.id",
					"tableA.name",
					"tableA.other_b.tableB.name",
				},
				Limit: 5},
			Expected: QueryResult{
				Data: []map[string]any{
					{"tableA.id": int32(4), "tableA.name": "Alice", "tableA.other_b.tableB.name": "nameB1"},
					{"tableA.id": int32(5), "tableA.name": "Bob", "tableA.other_b.tableB.name": "nameB2"},
					{"tableA.id": int32(6), "tableA.name": "Charlie", "tableA.other_b.tableB.name": "nameB2"},
				},
				Limit: 5,
				Total: 3,
			},
		},
		{
			Desc: "select columns from a and b with filter on tableA column",
			Query: Query{
				Select: []ColumnSelector{
					"tableA.id",
					"tableA.name",
					"tableA.other_b.tableB.name",
				},
				Where: &WhereExpression{
					Filter: &Filter{
						Column:   "tableA.name",
						Operator: "equal",
						Value:    "Bob"}},
				Limit: 5},
			Expected: QueryResult{
				Data: []map[string]any{
					{"tableA.id": int32(5), "tableA.name": "Bob", "tableA.other_b.tableB.name": "nameB2"},
				},
				Limit: 5,
				Total: 1,
			},
		},
		{
			Desc: "select columns from a and b with filter on tableB column",
			Query: Query{
				Select: []ColumnSelector{
					"tableA.id",
					"tableA.name",
					"tableA.other_b.tableB.name",
				},
				Where: &WhereExpression{
					Filter: &Filter{
						Column:   "tableA.other_b.tableB.name",
						Operator: "equal",
						Value:    "nameB1"}},
				Limit: 5},
			Expected: QueryResult{
				Data: []map[string]any{
					{"tableA.id": int32(4), "tableA.name": "Alice", "tableA.other_b.tableB.name": "nameB1"},
				},
				Limit: 5,
				Total: 1,
			},
		},
		{
			Desc: "select columns from a, b and c",
			Query: Query{
				Select: []ColumnSelector{
					"tableA.id",
					"tableA.other_b.tableB.id",
					"tableA.other_b.tableB.other_c.tableC.name",
					"tableA.other_b.tableB.other_c.tableC.description",
					"tableA.other_b2.tableB.other_c.tableC.description",
				},
				Limit: 5,
			},
			Expected: QueryResult{
				Data: []map[string]any{
					{"tableA.id": int32(4), "tableA.other_b.tableB.id": int32(1), "tableA.other_b.tableB.other_c.tableC.description": "Description 1", "tableA.other_b.tableB.other_c.tableC.name": "tableC1", "tableA.other_b2.tableB.other_c.tableC.description": "Description 2"},
					{"tableA.id": int32(5), "tableA.other_b.tableB.id": int32(2), "tableA.other_b.tableB.other_c.tableC.description": "Description 2", "tableA.other_b.tableB.other_c.tableC.name": "tableC2", "tableA.other_b2.tableB.other_c.tableC.description": nil},
					{"tableA.id": int32(6), "tableA.other_b.tableB.id": int32(2), "tableA.other_b.tableB.other_c.tableC.description": "Description 2", "tableA.other_b.tableB.other_c.tableC.name": "tableC2", "tableA.other_b2.tableB.other_c.tableC.description": nil},
				},
				Limit: 5,
				Total: 3,
			},
		},
		{
			Desc: "select columns from a, b and c with filter on b",
			Query: Query{
				Select: []ColumnSelector{
					"tableA.id",
					"tableA.other_b.tableB.id",
					"tableA.other_b.tableB.other_c.tableC.name",
					"tableA.other_b.tableB.other_c.tableC.description",
				},
				Where: &WhereExpression{
					Or: []WhereExpression{
						{Filter: &Filter{
							Column:   "tableA.other_b.tableB.id",
							Operator: "equal",
							Value:    nil,
						}},
						{Filter: &Filter{
							Column:   "tableA.other_b.tableB.id",
							Operator: "notEqual",
							Value:    1,
						}},
					}},
				Limit: 5,
			},
			Expected: QueryResult{
				Data: []map[string]any{
					{"tableA.id": int32(5), "tableA.other_b.tableB.id": int32(2), "tableA.other_b.tableB.other_c.tableC.description": "Description 2", "tableA.other_b.tableB.other_c.tableC.name": "tableC2"},
					{"tableA.id": int32(6), "tableA.other_b.tableB.id": int32(2), "tableA.other_b.tableB.other_c.tableC.description": "Description 2", "tableA.other_b.tableB.other_c.tableC.name": "tableC2"},
				},
				Limit: 5,
				Total: 2,
			},
		},
		{
			Desc: "select columns from a, b and c with filter on c",
			Query: Query{
				Select: []ColumnSelector{
					"tableA.id",
					"tableA.other_b.tableB.id",
					"tableA.other_b.tableB.other_c.tableC.name",
					"tableA.other_b.tableB.other_c.tableC.description",
				},
				Where: &WhereExpression{
					Filter: &Filter{
						Column:   "tableA.other_b.tableB.other_c.tableC.description",
						Operator: "contains",
						Value:    " ",
					},
				},
				Limit: 5,
			},
			Expected: QueryResult{
				Data: []map[string]any{
					{"tableA.id": int32(4), "tableA.other_b.tableB.id": int32(1), "tableA.other_b.tableB.other_c.tableC.description": "Description 1", "tableA.other_b.tableB.other_c.tableC.name": "tableC1"},
					{"tableA.id": int32(5), "tableA.other_b.tableB.id": int32(2), "tableA.other_b.tableB.other_c.tableC.description": "Description 2", "tableA.other_b.tableB.other_c.tableC.name": "tableC2"},
					{"tableA.id": int32(6), "tableA.other_b.tableB.id": int32(2), "tableA.other_b.tableB.other_c.tableC.description": "Description 2", "tableA.other_b.tableB.other_c.tableC.name": "tableC2"},
				},
				Limit: 5,
				Total: 3,
			},
		},
	}

	schema := `
DROP TABLE IF EXISTS "tableA";
DROP TABLE IF EXISTS "tableB";
DROP TABLE IF EXISTS "tableC";

CREATE TABLE "tableC" (
  name TEXT NOT NULL PRIMARY KEY,
  description TEXT
);

CREATE TABLE "tableB" (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  other_c TEXT REFERENCES "tableC"(name) -- nullable
);

CREATE TABLE "tableA" (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  age DOUBLE PRECISION,
  other_b INTEGER REFERENCES "tableB"(id) NOT NULL,
	other_b2 INTEGER REFERENCES "tableB"(id)
);

INSERT INTO "tableC" (name, description) VALUES
  ('tableC1', 'Description 1'),
  ('tableC2', 'Description 2'),
  ('tableC3', 'Description 3');

INSERT INTO "tableB" (id, name, other_c) VALUES
  (1, 'nameB1', 'tableC1'),
  (2, 'nameB2', 'tableC2'),
  (3, 'nameB3', NULL);

INSERT INTO "tableA" (id, name, age, other_b, other_b2) VALUES
  (4, 'Alice', 30, 1, 2),
  (5, 'Bob', 25, 2, NULL),
  (6, 'Charlie', 35, 2, 3);
`

	c := Config{
		ColumnDefaults: map[DataType]ColumnBehavior{
			DataType("integer"): {
				AllowSorting:     true,
				AllowFiltering:   false,
				FilterOperations: []FilterOperator{"equal", "notEqual", "greater", "greaterOrEqual", "less", "lessOrEqual"},
			},
			DataType("text"): {
				AllowSorting:     false,
				AllowFiltering:   true,
				FilterOperations: []FilterOperator{"equal", "notEqual", "greater", "greaterOrEqual", "less", "lessOrEqual"},
			},
			DataType("double precision"): {
				AllowSorting:     false,
				AllowFiltering:   false,
				FilterOperations: []FilterOperator{"equal"},
			}},
		ColumnUnknownDefault: ColumnBehavior{
			AllowSorting:     false,
			AllowFiltering:   false,
			FilterOperations: nil,
		}}

	api, err := NewAPI(c)
	if err != nil {
		t.Fatalf("Failed to create API: %v", err)
	}

	db, err := getTestDB(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}
	defer db.Close(ctx)

	Convey("Apply schema", t, func() {
		_, err = db.Exec(ctx, schema)
		So(err, ShouldBeNil)

		Convey("Discover from base tableA", func() {
			tables, err := api.Discover(ctx, db, "tableA")
			So(err, ShouldBeNil)

			Convey("should have table metadata", func() {
				So(tables, ShouldHaveLength, 3)
			})

			for idx, tc := range tcs {
				Convey(fmt.Sprintf("index %d, %s", idx, tc.Desc), func() {
					result, _, err := api.Query(ctx, db, tables, tc.Query)
					//result, debug, err := api.Query(ctx, db, tables, tc.Query)
					// if debug.PageSQL != "" {
					// 	Printf("debug page sql: '%s'\ntotal sql: '%s'\n", debug.PageSQL, debug.TotalSQL)
					// }
					So(err, ShouldBeNil)

					Convey("should have query result", func() {
						So(result, ShouldResemble, tc.Expected)
					})

					Convey("should have ...", func() {
						Convey("data length", func() {
							So(result.Data, ShouldHaveLength, len(tc.Expected.Data))
						})
						if len(result.Data) > 0 && len(tc.Expected.Data) > 0 {
							Convey("checking whether data types in first row matches", func() {
								xs := result.Data[0]
								ys := tc.Expected.Data[0]

								Convey("should have same number of entries", func() {
									So(xs, ShouldHaveLength, len(ys))
								})

								for k, v := range xs {
									Convey(fmt.Sprintf("expected data should have key '%s'", k), func() {
										So(ys, ShouldContainKey, k)

										Convey("value should be equal", func() {
											So(v, ShouldEqual, ys[k])
										})
										Convey("value should have same type", func() {
											So(v, ShouldHaveSameTypeAs, ys[k])
										})
									})
								}
							})
						}
						Convey("limit", func() {
							So(result.Limit, ShouldEqual, tc.Expected.Limit)
						})
						Convey("total", func() {
							So(result.Total, ShouldEqual, tc.Expected.Total)
						})
					})
				})
			}
		})
	})
}
