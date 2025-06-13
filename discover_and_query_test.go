package pgd

import (
	"context"
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

// uses table names: tableA, tableB, tableC

type testCase struct {
	Desc     string
	Query    Query
	Expected QueryResult
}

func TestDiscoverAndQueryData(t *testing.T) {
	ctx := context.Background()

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
  other_b2 INTEGER REFERENCES "tableB"(id),
  xs TEXT[]
);

INSERT INTO "tableC" (name, description) VALUES
  ('tableC1', 'Description 1'),
  ('tableC2', 'Description 2'),
  ('tableC3', 'Description 3');

INSERT INTO "tableB" (id, name, other_c) VALUES
  (1, 'nameB1', 'tableC1'),
  (2, 'nameB2', 'tableC2'),
  (3, 'nameB3', NULL);

INSERT INTO "tableA" (id, name, age, other_b, other_b2, xs) VALUES
  (4, 'Alice', 30, 1, 2, '{"xx", "yy"}'),
  (5, 'Bob', 25, 2, NULL, '{"xx"}'),
  (6, 'Charlie', 35, 2, 3, NULL);
`
	c := Config{
		FilterOperations: DefaultFilterOperations,
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
			},
			DataType("text[]"): {
				AllowSorting:     true,
				AllowFiltering:   true,
				FilterOperations: []FilterOperator{"any"},
			},
		},
		ColumnUnknownDefault: ColumnBehavior{
			AllowSorting:     false,
			AllowFiltering:   false,
			FilterOperations: nil,
		}}

	filterInt := []FilterOperator{
		"equal",
		"notEqual",
		"greater",
		"greaterOrEqual",
		"less",
		"lessOrEqual",
	}
	filterText := filterInt
	filterDouble := []FilterOperator{"equal"}

	expectedTables := TablesMetadata{
		"tableA": TableMetadata{
			Name: "tableA",
			Columns: map[Column]ColumnMetadata{
				"id": {
					Name:       "id",
					DataType:   "integer",
					IsNullable: false,
					Behavior: ColumnBehavior{
						AllowSorting:     true,
						AllowFiltering:   false,
						FilterOperations: filterInt,
					},
				},
				"name": {
					Name:       "name",
					DataType:   "text",
					IsNullable: false,
					Behavior: ColumnBehavior{
						AllowSorting:     false,
						AllowFiltering:   true,
						FilterOperations: filterText,
					},
				},
				"age": {
					Name:       "age",
					DataType:   "double precision",
					IsNullable: true,
					Behavior: ColumnBehavior{
						AllowSorting:     false,
						AllowFiltering:   false,
						FilterOperations: filterDouble,
					},
				},
				"other_b": {
					Name:       "other_b",
					DataType:   "integer",
					IsNullable: false,
					Relation: &ColumnRelation{
						Table:  "tableB",
						Column: "id",
					},
					Behavior: ColumnBehavior{
						AllowSorting:     true,
						AllowFiltering:   false,
						FilterOperations: filterInt,
					},
				},
				"other_b2": {
					Name:       "other_b2",
					DataType:   "integer",
					IsNullable: true,
					Relation: &ColumnRelation{
						Table:  "tableB",
						Column: "id",
					},
					Behavior: ColumnBehavior{
						AllowSorting:     true,
						AllowFiltering:   false,
						FilterOperations: filterInt,
					},
				},
				"xs": {
					Name:       "xs",
					DataType:   "text[]",
					IsNullable: true,
					Behavior: ColumnBehavior{
						AllowSorting:     true,
						AllowFiltering:   true,
						FilterOperations: []FilterOperator{"any"},
					},
				},
			},
			Behavior: TableBehavior{},
		},
		"tableB": TableMetadata{
			Name: "tableB",
			Columns: map[Column]ColumnMetadata{
				"id": {
					Name:       "id",
					DataType:   "integer",
					IsNullable: false,
					Behavior: ColumnBehavior{
						AllowSorting:     true,
						AllowFiltering:   false,
						FilterOperations: filterInt,
					},
				},
				"name": {
					Name:       "name",
					DataType:   "text",
					IsNullable: false,
					Behavior: ColumnBehavior{
						AllowSorting:     false,
						AllowFiltering:   true,
						FilterOperations: filterText,
					},
				},
				"other_c": {
					Name:       "other_c",
					DataType:   "text",
					IsNullable: true,
					Relation: &ColumnRelation{
						Table:  "tableC",
						Column: "name",
					},
					Behavior: ColumnBehavior{
						AllowSorting:     false,
						AllowFiltering:   true,
						FilterOperations: filterText,
					},
				},
			},
			Behavior: TableBehavior{},
		},
		"tableC": TableMetadata{
			Name: "tableC",
			Columns: map[Column]ColumnMetadata{
				"name": {
					Name:       "name",
					DataType:   "text",
					IsNullable: false,
					Behavior: ColumnBehavior{
						AllowSorting:     false,
						AllowFiltering:   true,
						FilterOperations: filterText,
					},
				},
				"description": {
					Name:       "description",
					DataType:   "text",
					IsNullable: true,
					Behavior: ColumnBehavior{
						AllowSorting:     false,
						AllowFiltering:   true,
						FilterOperations: filterText,
					},
				},
			},
			Behavior: TableBehavior{},
		},
	}
	tcs := []testCase{
		{
			Desc: "Select all columns from tableA",
			Query: Query{
				Select: []ColumnSelector{
					"id",
					"name",
					"age",
					"other_b",
					"other_b2",
				},
				From:  "tableA",
				Limit: 5},
			Expected: QueryResult{
				Data: []map[string]any{
					{"id": int32(4), "name": "Alice", "age": 30.0, "other_b": int32(1), "other_b2": int32(2)},
					{"id": int32(5), "name": "Bob", "age": 25.0, "other_b": int32(2), "other_b2": nil},
					{"id": int32(6), "name": "Charlie", "age": 35.0, "other_b": int32(2), "other_b2": int32(3)},
				},
				Limit: 5,
				Total: 3,
			},
		},
		{
			Desc: "select columns from a, simple filter",
			Query: Query{
				Select: []ColumnSelector{
					"id",
					"name",
				},
				From: "tableA",
				Where: &WhereExpression{
					Filter: &Filter{
						Column:   "name",
						Operator: "equal",
						Value:    "Bob"},
				},
				Limit: 5},
			Expected: QueryResult{
				Data: []map[string]any{
					{"id": int32(5), "name": "Bob"},
				},
				Limit: 5,
				Total: 1,
			},
		},
		{
			Desc: "select some columns from a and b",
			Query: Query{
				Select: []ColumnSelector{
					"id",
					"name",
					"other_b.name",
				},
				From:  "tableA",
				Limit: 5},
			Expected: QueryResult{
				Data: []map[string]any{
					{"id": int32(4), "name": "Alice", "other_b.name": "nameB1"},
					{"id": int32(5), "name": "Bob", "other_b.name": "nameB2"},
					{"id": int32(6), "name": "Charlie", "other_b.name": "nameB2"},
				},
				Limit: 5,
				Total: 3,
			},
		},
		{
			Desc: "select columns from a and b with filter on tableA column",
			Query: Query{
				Select: []ColumnSelector{
					"id",
					"name",
					"other_b.name",
				},
				From: "tableA",
				Where: &WhereExpression{
					Filter: &Filter{
						Column:   "name",
						Operator: "equal",
						Value:    "Bob"}},
				Limit: 5},
			Expected: QueryResult{
				Data: []map[string]any{
					{"id": int32(5), "name": "Bob", "other_b.name": "nameB2"},
				},
				Limit: 5,
				Total: 1,
			},
		},
		{
			Desc: "select columns from a and b with filter on tableB column",
			Query: Query{
				Select: []ColumnSelector{
					"id",
					"name",
					"other_b.name",
				},
				From: "tableA",
				Where: &WhereExpression{
					Filter: &Filter{
						Column:   "other_b.name",
						Operator: "equal",
						Value:    "nameB1"}},
				Limit: 5},
			Expected: QueryResult{
				Data: []map[string]any{
					{"id": int32(4), "name": "Alice", "other_b.name": "nameB1"},
				},
				Limit: 5,
				Total: 1,
			},
		},
		{
			Desc: "select columns from a, b and c",
			Query: Query{
				Select: []ColumnSelector{
					"id",
					"other_b.id",
					"other_b.other_c.name",
					"other_b.other_c.description",
					"other_b2.other_c.description",
				},
				From:  "tableA",
				Limit: 5,
			},
			Expected: QueryResult{
				Data: []map[string]any{
					{"id": int32(4), "other_b.id": int32(1), "other_b.other_c.description": "Description 1", "other_b.other_c.name": "tableC1", "other_b2.other_c.description": "Description 2"},
					{"id": int32(5), "other_b.id": int32(2), "other_b.other_c.description": "Description 2", "other_b.other_c.name": "tableC2", "other_b2.other_c.description": nil},
					{"id": int32(6), "other_b.id": int32(2), "other_b.other_c.description": "Description 2", "other_b.other_c.name": "tableC2", "other_b2.other_c.description": nil},
				},
				Limit: 5,
				Total: 3,
			},
		},
		{
			Desc: "select columns from a, b and c with filter on b",
			Query: Query{
				Select: []ColumnSelector{
					"id",
					"other_b.id",
					"other_b.other_c.name",
					"other_b.other_c.description",
				},
				From: "tableA",
				Where: &WhereExpression{
					Or: []WhereExpression{
						{Filter: &Filter{
							Column:   "other_b.id",
							Operator: "equal",
							Value:    nil,
						}},
						{Filter: &Filter{
							Column:   "other_b.id",
							Operator: "notEqual",
							Value:    1,
						}},
					}},
				Limit: 5,
			},
			Expected: QueryResult{
				Data: []map[string]any{
					{"id": int32(5), "other_b.id": int32(2), "other_b.other_c.description": "Description 2", "other_b.other_c.name": "tableC2"},
					{"id": int32(6), "other_b.id": int32(2), "other_b.other_c.description": "Description 2", "other_b.other_c.name": "tableC2"},
				},
				Limit: 5,
				Total: 2,
			},
		},
		{
			Desc: "select columns from a, b and c with filter on c",
			Query: Query{
				Select: []ColumnSelector{
					"id",
					"other_b.id",
					"other_b.other_c.name",
					"other_b.other_c.description",
				},
				From: "tableA",
				Where: &WhereExpression{
					Filter: &Filter{
						Column:   "other_b.other_c.description",
						Operator: "contains",
						Value:    " ",
					},
				},
				Limit: 5,
			},
			Expected: QueryResult{
				Data: []map[string]any{
					{"id": int32(4), "other_b.id": int32(1), "other_b.other_c.description": "Description 1", "other_b.other_c.name": "tableC1"},
					{"id": int32(5), "other_b.id": int32(2), "other_b.other_c.description": "Description 2", "other_b.other_c.name": "tableC2"},
					{"id": int32(6), "other_b.id": int32(2), "other_b.other_c.description": "Description 2", "other_b.other_c.name": "tableC2"},
				},
				Limit: 5,
				Total: 3,
			},
		},
		{
			Desc: "filter column 'xs' in tableA",
			Query: Query{
				Select: []ColumnSelector{"id", "xs"},
				From:   "tableA",
				Where: &WhereExpression{
					Filter: &Filter{
						Column:   "xs",
						Operator: "any",
						Value:    "xx"},
				},
				Limit: 5,
			},
			Expected: QueryResult{
				Data: []map[string]any{
					{"id": int32(4), "xs": []any{"xx", "yy"}},
					{"id": int32(5), "xs": []any{"xx"}}},
				Limit: 5, Total: 2},
		},
	}

	runTests(ctx, t, c, schema, "tableA", expectedTables, tcs)
}

func TestDiscoverAndQueryDataWithEnums(t *testing.T) {
	ctx := context.Background()

	schema := `
DROP TABLE IF EXISTS "tableD";

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_status') THEN
        CREATE TYPE user_status AS ENUM ('active', 'inactive', 'pending');
    END IF;
END
$$;

CREATE TABLE "tableD" (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  status user_status NOT NULL
);

INSERT INTO "tableD" (id, name, status) VALUES
  (1, 'Alice', 'active'),
  (2, 'Bob', 'inactive'),
  (3, 'Charlie', 'pending');
`

	filterInt := []FilterOperator{
		"equal",
		"notEqual",
		"greater",
		"greaterOrEqual",
		"less",
		"lessOrEqual",
	}
	filterTextWithContains := []FilterOperator{
		"equal",
		"notEqual",
		"contains",
	}
	filterEnum := []FilterOperator{
		"equal",
		"notEqual",
	}

	expectedTables := TablesMetadata{
		"tableD": TableMetadata{
			Name: "tableD",
			Columns: map[Column]ColumnMetadata{
				"id": {
					Name:       "id",
					DataType:   "integer",
					IsNullable: false,
					Behavior: ColumnBehavior{
						AllowSorting:     true,
						AllowFiltering:   true,
						FilterOperations: filterInt,
					},
				},
				"name": {
					Name:       "name",
					DataType:   "text",
					IsNullable: false,
					Behavior: ColumnBehavior{
						AllowSorting:     false,
						AllowFiltering:   true,
						FilterOperations: filterTextWithContains,
					},
				},
				"status": {
					Name:       "status",
					DataType:   "user_status",
					IsNullable: false,
					Behavior: ColumnBehavior{
						AllowSorting:     true,
						AllowFiltering:   true,
						FilterOperations: filterEnum,
					},
				},
			},
			Behavior: TableBehavior{},
		},
	}

	tcs := []testCase{
		{
			Desc: "Select all columns from tableD with enum",
			Query: Query{
				Select: []ColumnSelector{
					"id",
					"name",
					"status",
				},
				From:  "tableD",
				Limit: 5},
			Expected: QueryResult{
				Data: []map[string]any{
					{"id": int32(1), "name": "Alice", "status": "active"},
					{"id": int32(2), "name": "Bob", "status": "inactive"},
					{"id": int32(3), "name": "Charlie", "status": "pending"},
				},
				Limit: 5,
				Total: 3,
			},
		},
		{
			Desc: "Filter by enum value",
			Query: Query{
				Select: []ColumnSelector{
					"id",
					"name",
					"status",
				},
				From: "tableD",
				Where: &WhereExpression{
					Filter: &Filter{
						Column:   "status",
						Operator: "equal",
						Value:    "active",
					},
				},
				Limit: 5},
			Expected: QueryResult{
				Data: []map[string]any{
					{"id": int32(1), "name": "Alice", "status": "active"},
				},
				Limit: 5,
				Total: 1,
			},
		},
		{
			Desc: "Multiple enum filters with OR",
			Query: Query{
				Select: []ColumnSelector{
					"id",
					"name",
					"status",
				},
				From: "tableD",
				Where: &WhereExpression{
					Or: []WhereExpression{
						{Filter: &Filter{
							Column:   "status",
							Operator: "equal",
							Value:    "active",
						}},
						{Filter: &Filter{
							Column:   "status",
							Operator: "equal",
							Value:    "pending",
						}},
					},
				},
				Limit: 5},
			Expected: QueryResult{
				Data: []map[string]any{
					{"id": int32(1), "name": "Alice", "status": "active"},
					{"id": int32(3), "name": "Charlie", "status": "pending"},
				},
				Limit: 5,
				Total: 2,
			},
		},
	}

	c := Config{
		FilterOperations: DefaultFilterOperations,
		ColumnDefaults: map[DataType]ColumnBehavior{
			DataType("integer"): {
				AllowSorting:     true,
				AllowFiltering:   true,
				FilterOperations: []FilterOperator{"equal", "notEqual", "greater", "greaterOrEqual", "less", "lessOrEqual"},
			},
			DataType("text"): {
				AllowSorting:     false,
				AllowFiltering:   true,
				FilterOperations: []FilterOperator{"equal", "notEqual", "contains"},
			},
			DataType("user_status"): {
				AllowSorting:     true,
				AllowFiltering:   true,
				FilterOperations: []FilterOperator{"equal", "notEqual"},
			},
		},
		ColumnUnknownDefault: ColumnBehavior{
			AllowSorting:     false,
			AllowFiltering:   false,
			FilterOperations: nil,
		},
	}

	runTests(ctx, t, c, schema, "tableD", expectedTables, tcs)
}

func runTests(ctx context.Context, t *testing.T, c Config, schema string, baseTable Table, expectedTables TablesMetadata, tcs []testCase) {

	api, err := NewAPI(c)
	if err != nil {
		t.Fatalf("Failed to create API: %v", err)
	}

	db, err := getTestDB(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}
	defer db.Close(ctx)

	Convey(fmt.Sprintf("Unit test %s, base table %s. Apply schema", t.Name(), baseTable), t, func() {
		_, err = db.Exec(ctx, schema)
		So(err, ShouldBeNil)

		Convey("Discover from base", func() {
			result, err := api.Discover(ctx, db, baseTable)
			So(err, ShouldBeNil)

			Convey("should have table metadata", func() {
				So(result.TablesMetadata, ShouldResemble, expectedTables)
			})

			for idx, tc := range tcs {
				Convey(fmt.Sprintf("index %d, %s", idx, tc.Desc), func() {
					//result, _, err := api.Query(ctx, db, tables, tc.Query)
					result, debug, err := api.Query(ctx, db, result.TablesMetadata, tc.Query)
					if debug.PageSQL != "" {
						Printf("debug page sql: '%s'\nargs: '%v', total sql: '%s'\n", debug.PageSQL, debug.PageArgs, debug.TotalSQL)
					}
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
