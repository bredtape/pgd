package pgd

import (
	"fmt"
	"slices"
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
			"integer": {
				AllowSorting:   true,
				AllowFiltering: false,
			},
			"text": {
				AllowSorting:     false,
				AllowFiltering:   true,
				FilterOperations: []FilterOperator{"contains", "equals", "notEquals", "notContains"},
			},
			"double precision": {
				AllowSorting:     false,
				AllowFiltering:   true,
				FilterOperations: []FilterOperator{"equals"},
			},
			"text[]": {
				AllowSorting:     true,
				AllowFiltering:   true,
				FilterOperations: []FilterOperator{"containsElement"},
			},
		},
	}

	// filterInt := []FilterOperator{
	// 	"equals",
	// 	"notEquals",
	// 	"greater",
	// 	"greaterOrEquals",
	// 	"less",
	// 	"lessOrEquals",
	// }
	filterText := sortedSlice([]FilterOperator{"equals", "notEquals", "contains", "notContains"})
	filterDouble := []FilterOperator{"equals"}

	expectedTables := TablesMetadata{
		"tableA": TableMetadata{
			Name: "tableA",
			Columns: map[Column]ColumnMetadata{
				"id": {
					Name:       "id",
					Table:      "tableA",
					DataType:   "integer",
					IsNullable: false,
					Behavior: ColumnBehavior{
						AllowSorting:   true,
						AllowFiltering: false,
					},
				},
				"name": {
					Name:       "name",
					Table:      "tableA",
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
					Table:      "tableA",
					DataType:   "double precision",
					IsNullable: true,
					Behavior: ColumnBehavior{
						AllowSorting:     false,
						AllowFiltering:   true,
						FilterOperations: filterDouble,
					},
				},
				"other_b": {
					Name:       "other_b",
					Table:      "tableA",
					DataType:   "integer",
					IsNullable: false,
					Relation: &ColumnRelation{
						Table:  "tableB",
						Column: "id",
					},
					Behavior: ColumnBehavior{
						AllowSorting:   true,
						AllowFiltering: false,
					},
				},
				"other_b2": {
					Name:       "other_b2",
					Table:      "tableA",
					DataType:   "integer",
					IsNullable: true,
					Relation: &ColumnRelation{
						Table:  "tableB",
						Column: "id",
					},
					Behavior: ColumnBehavior{
						AllowSorting:   true,
						AllowFiltering: false,
					},
				},
				"xs": {
					Name:       "xs",
					Table:      "tableA",
					DataType:   "text[]",
					IsNullable: true,
					Behavior: ColumnBehavior{
						AllowSorting:     true,
						AllowFiltering:   true,
						FilterOperations: []FilterOperator{"containsElement"},
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
					Table:      "tableB",
					DataType:   "integer",
					IsNullable: false,
					Behavior: ColumnBehavior{
						AllowSorting:   true,
						AllowFiltering: false,
					},
				},
				"name": {
					Name:       "name",
					Table:      "tableB",
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
					Table:      "tableB",
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
					Table:      "tableC",
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
					Table:      "tableC",
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
						Operator: "equals",
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
					"other_b",
					"other_b.name",
				},
				From:  "tableA",
				Limit: 5},
			Expected: QueryResult{
				Data: []map[string]any{
					{"id": int32(4), "name": "Alice", "other_b.name": "nameB1", "other_b": int32(1)},
					{"id": int32(5), "name": "Bob", "other_b.name": "nameB2", "other_b": int32(2)},
					{"id": int32(6), "name": "Charlie", "other_b.name": "nameB2", "other_b": int32(2)},
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
						Operator: "equals",
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
						Operator: "equals",
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
							Operator: "equals",
							Value:    nil,
						}},
						{Filter: &Filter{
							Column:   "other_b.id",
							Operator: "notEquals",
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
						Operator: "containsElement",
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

	runTests(t, c, schema, "tableA", expectedTables, tcs)
}

func TestDiscoverAndQueryWithVeryLongTableAndColumnNames(t *testing.T) {
	schema := `
DROP TABLE IF EXISTS "table_very_long_table_prefix_but_below_63_bytes_A";
DROP TABLE IF EXISTS "table_very_long_table_prefix_but_below_63_bytes_B";
DROP TABLE IF EXISTS "table_very_long_table_prefix_but_below_63_bytes_C";

CREATE TABLE "table_very_long_table_prefix_but_below_63_bytes_C" (
  very_long_column_name_very_long_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE "table_very_long_table_prefix_but_below_63_bytes_B" (
  id INTEGER PRIMARY KEY,
  very_long_column_name_very_long_column_name_very_long_name TEXT NOT NULL,
	very_long_column_name_very_long_column_name_very_long_other_c INTEGER REFERENCES "table_very_long_table_prefix_but_below_63_bytes_C"(very_long_column_name_very_long_id)
);

CREATE TABLE "table_very_long_table_prefix_but_below_63_bytes_A" (
  id INTEGER PRIMARY KEY,
	very_long_column_name_very_long_column_name_very_long_other_b INTEGER REFERENCES "table_very_long_table_prefix_but_below_63_bytes_B"(id)
);

INSERT INTO "table_very_long_table_prefix_but_below_63_bytes_C" (very_long_column_name_very_long_id, name) VALUES
  (1, 'nameC1'),
  (2, 'nameC2'),
  (3, 'nameC3');

INSERT INTO "table_very_long_table_prefix_but_below_63_bytes_B" (id, very_long_column_name_very_long_column_name_very_long_name, very_long_column_name_very_long_column_name_very_long_other_c) VALUES
  (1, 'nameB1', 2),
  (2, 'nameB2', 2),
  (3, 'nameB3', 3);

INSERT INTO "table_very_long_table_prefix_but_below_63_bytes_A" (id, very_long_column_name_very_long_column_name_very_long_other_b) VALUES
  (4, 2),
  (5, 3),
  (6, NULL);
`

	c := Config{
		FilterOperations: DefaultFilterOperations,
		ColumnDefaults: map[DataType]ColumnBehavior{
			"integer": ColumnBehavior{},
			"text":    ColumnBehavior{},
		}}

	expectedTables := TablesMetadata{
		"table_very_long_table_prefix_but_below_63_bytes_A": TableMetadata{
			Name: "table_very_long_table_prefix_but_below_63_bytes_A",
			Columns: map[Column]ColumnMetadata{
				"id": {
					Name:     "id",
					Table:    "table_very_long_table_prefix_but_below_63_bytes_A",
					DataType: "integer"},
				"very_long_column_name_very_long_column_name_very_long_other_b": {
					Name:       "very_long_column_name_very_long_column_name_very_long_other_b",
					Table:      "table_very_long_table_prefix_but_below_63_bytes_A",
					DataType:   "integer",
					IsNullable: true,
					Relation: &ColumnRelation{
						Table:  "table_very_long_table_prefix_but_below_63_bytes_B",
						Column: "id"}}}},
		"table_very_long_table_prefix_but_below_63_bytes_B": TableMetadata{
			Name: "table_very_long_table_prefix_but_below_63_bytes_B",
			Columns: map[Column]ColumnMetadata{
				"id": {
					Name:     "id",
					Table:    "table_very_long_table_prefix_but_below_63_bytes_B",
					DataType: "integer"},
				"very_long_column_name_very_long_column_name_very_long_name": {
					Name:     "very_long_column_name_very_long_column_name_very_long_name",
					Table:    "table_very_long_table_prefix_but_below_63_bytes_B",
					DataType: "text"},
				"very_long_column_name_very_long_column_name_very_long_other_c": {
					Name:       "very_long_column_name_very_long_column_name_very_long_other_c",
					Table:      "table_very_long_table_prefix_but_below_63_bytes_B",
					DataType:   "integer",
					IsNullable: true,
					Relation: &ColumnRelation{
						Table:  "table_very_long_table_prefix_but_below_63_bytes_C",
						Column: "very_long_column_name_very_long_id"}}},
		},
		"table_very_long_table_prefix_but_below_63_bytes_C": TableMetadata{
			Name: "table_very_long_table_prefix_but_below_63_bytes_C",
			Columns: map[Column]ColumnMetadata{
				"name": {
					Name:     "name",
					Table:    "table_very_long_table_prefix_but_below_63_bytes_C",
					DataType: "text"},
				"very_long_column_name_very_long_id": {
					Name:     "very_long_column_name_very_long_id",
					Table:    "table_very_long_table_prefix_but_below_63_bytes_C",
					DataType: "integer"}}}}

	tcs := []testCase{
		{
			Desc: "Select columns from all tables",
			Query: Query{
				Select: []ColumnSelector{
					"id",
					"very_long_column_name_very_long_column_name_very_long_other_b",
					"very_long_column_name_very_long_column_name_very_long_other_b.very_long_column_name_very_long_column_name_very_long_name",
					"very_long_column_name_very_long_column_name_very_long_other_b.very_long_column_name_very_long_column_name_very_long_other_c",
					"very_long_column_name_very_long_column_name_very_long_other_b.very_long_column_name_very_long_column_name_very_long_other_c.very_long_column_name_very_long_id",
				},
				From:  "table_very_long_table_prefix_but_below_63_bytes_A",
				Limit: 5,
			},
			Expected: QueryResult{
				Data: []map[string]any{
					{"id": int32(4), "very_long_column_name_very_long_column_name_very_long_other_b": int32(2), "very_long_column_name_very_long_column_name_very_long_other_b.very_long_column_name_very_long_column_name_very_long_name": "nameB2", "very_long_column_name_very_long_column_name_very_long_other_b.very_long_column_name_very_long_column_name_very_long_other_c": int32(2), "very_long_column_name_very_long_column_name_very_long_other_b.very_long_column_name_very_long_column_name_very_long_other_c.very_long_column_name_very_long_id": int32(2)},
					{"id": int32(5), "very_long_column_name_very_long_column_name_very_long_other_b": int32(3), "very_long_column_name_very_long_column_name_very_long_other_b.very_long_column_name_very_long_column_name_very_long_name": "nameB3", "very_long_column_name_very_long_column_name_very_long_other_b.very_long_column_name_very_long_column_name_very_long_other_c": int32(3), "very_long_column_name_very_long_column_name_very_long_other_b.very_long_column_name_very_long_column_name_very_long_other_c.very_long_column_name_very_long_id": int32(3)},
					{"id": int32(6), "very_long_column_name_very_long_column_name_very_long_other_b": any(nil), "very_long_column_name_very_long_column_name_very_long_other_b.very_long_column_name_very_long_column_name_very_long_name": any(nil), "very_long_column_name_very_long_column_name_very_long_other_b.very_long_column_name_very_long_column_name_very_long_other_c": any(nil), "very_long_column_name_very_long_column_name_very_long_other_b.very_long_column_name_very_long_column_name_very_long_other_c.very_long_column_name_very_long_id": any(nil)}},
				Limit: 5, Total: 3}}}

	runTests(t, c, schema, "table_very_long_table_prefix_but_below_63_bytes_A", expectedTables, tcs)
}

func TestDiscoverAndQueryDataWithEnums(t *testing.T) {
	schema := `
DROP TABLE IF EXISTS "tableD" CASCADE;

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

	filterInt := sortedSlice([]FilterOperator{
		"equals",
		"notEquals",
		"greater",
		"greaterOrEquals",
		"less",
		"lessOrEquals"})
	filterTextWithContains := sortedSlice([]FilterOperator{
		"equals",
		"notEquals",
		"contains"})
	filterEnum := []FilterOperator{
		"equals",
		"notEquals"}

	expectedTables := TablesMetadata{
		"tableD": TableMetadata{
			Name: "tableD",
			Columns: map[Column]ColumnMetadata{
				"id": {
					Name:       "id",
					Table:      "tableD",
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
					Table:      "tableD",
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
					Table:      "tableD",
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
						Operator: "equals",
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
							Operator: "equals",
							Value:    "active",
						}},
						{Filter: &Filter{
							Column:   "status",
							Operator: "equals",
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
		FilterOperations: MergeUniqueMaps(DefaultFilterOperations, FilterOperations{
			"user_status": EqualsFilterOperations,
		}),
		ColumnDefaults: map[DataType]ColumnBehavior{
			"integer": {
				AllowSorting:     true,
				AllowFiltering:   true,
				FilterOperations: []FilterOperator{"equals", "notEquals", "greater", "greaterOrEquals", "less", "lessOrEquals"},
			},
			"text": {
				AllowSorting:     false,
				AllowFiltering:   true,
				FilterOperations: []FilterOperator{"equals", "notEquals", "contains"},
			},
			"user_status": {
				AllowSorting:     true,
				AllowFiltering:   true,
				FilterOperations: []FilterOperator{"equals", "notEquals"},
			},
		},
	}

	runTests(t, c, schema, "tableD", expectedTables, tcs)
}

// with tableA having optional relation with tableB, but tableB have required relation with tableC, then
// LEFT JOINs must be used all the way (or otherwise group the INNER JOINs inside the LEFT JOIN)
func TestDiscoverAndQueryWithOptionalReferenceHavingRequiredChild(t *testing.T) {
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
  other_c TEXT REFERENCES "tableC"(name) NOT NULL
);

CREATE TABLE "tableA" (
  id INTEGER PRIMARY KEY,
  other_b INTEGER REFERENCES "tableB"(id)
);

INSERT INTO "tableC" (name, description) VALUES
  ('tableC1', 'Description 1'),
  ('tableC2', 'Description 2'),
  ('tableC3', 'Description 3');

INSERT INTO "tableB" (id, other_c) VALUES
  (1, 'tableC1'),
  (2, 'tableC2'),
  (3, 'tableC3');

INSERT INTO "tableA" (id, other_b) VALUES
  (4, 1),
  (5, 2),
  (6, NULL);
	`

	c := Config{
		FilterOperations: DefaultFilterOperations,
		ColumnDefaults: map[DataType]ColumnBehavior{
			"integer": {},
			"text":    {},
		}}

	expectedTables := TablesMetadata{
		"tableA": TableMetadata{
			Name: "tableA",
			Columns: map[Column]ColumnMetadata{
				"id": {
					Name:     "id",
					Table:    "tableA",
					DataType: "integer",
				},
				"other_b": {
					Name:       "other_b",
					Table:      "tableA",
					DataType:   "integer",
					IsNullable: true,
					Relation: &ColumnRelation{
						Table:  "tableB",
						Column: "id",
					},
				},
			},
			Behavior: TableBehavior{},
		},
		"tableB": TableMetadata{
			Name: "tableB",
			Columns: map[Column]ColumnMetadata{
				"id": {
					Name:     "id",
					Table:    "tableB",
					DataType: "integer",
				},
				"other_c": {
					Name:     "other_c",
					Table:    "tableB",
					DataType: "text",
					Relation: &ColumnRelation{
						Table:  "tableC",
						Column: "name",
					},
				},
			},
			Behavior: TableBehavior{},
		},
		"tableC": TableMetadata{
			Name: "tableC",
			Columns: map[Column]ColumnMetadata{
				"name": {
					Name:     "name",
					Table:    "tableC",
					DataType: "text",
				},
				"description": {
					Name:       "description",
					Table:      "tableC",
					DataType:   "text",
					IsNullable: true,
				},
			},
			Behavior: TableBehavior{},
		},
	}

	tcs := []testCase{
		{
			Desc: "Select column from table A and C. Should have 3 rows",
			Query: Query{
				Select: []ColumnSelector{
					"id",
					"other_b.other_c.name",
				},
				From:  "tableA",
				Limit: 5},
			Expected: QueryResult{
				Data: []map[string]any{
					{"id": int32(4), "other_b.other_c.name": "tableC1"},
					{"id": int32(5), "other_b.other_c.name": "tableC2"},
					{"id": int32(6), "other_b.other_c.name": nil},
				},
				Limit: 5,
				Total: 3,
			},
		},
	}

	runTests(t, c, schema, "tableA", expectedTables, tcs)
}

func runTests(t *testing.T, c Config, schema string, baseTable Table, expectedTables TablesMetadata, tcs []testCase) {
	ctx := t.Context()

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

			if expectedTables != nil {
				shouldResembleTablesMetadata(result.TablesMetadata, expectedTables)

				// Convey("should have expected table", func() {
				// 	So(getMapKeys(result.TablesMetadata), ShouldResemble, getMapKeys(expectedTables))

				for _, k := range getMapKeys(expectedTables) {
					expectedTable := expectedTables[k]
					actualTable := result.TablesMetadata[k]
					Convey("table "+k.String(), func() {
						Convey("should have same colums", func() {
							So(getMapKeys(expectedTable.Columns), ShouldResemble, getMapKeys(actualTable.Columns))

							for _, c := range getMapKeys(expectedTable.Columns) {
								expectedMeta := expectedTable.Columns[c]
								Convey("column "+c.String(), func() {
									So(expectedMeta, ShouldResemble, actualTable.Columns[c])
								})
							}
						})
					})
				}

				// var errs []error
				// for k, table := range result.TablesMetadata {
				// 	for c, meta := range table.Columns {
				// 		if meta.Behavior.AllowFiltering && len(meta.Behavior.FilterOperations) == 0 {
				// 			errs = append(errs, fmt.Errorf("table %s, column %s, when AllowFiltering, FilterOperations should not be empty", k, c))
				// 		}
				// 		if !meta.Behavior.AllowFiltering && len(meta.Behavior.FilterOperations) > 0 {
				// 			errs = append(errs, fmt.Errorf("table %s, column %s, when NOT AllowFiltering, FilterOperations should be empty", k, c))
				// 		}
				// 	}
				// }
				// Convey("should have no column metadata errors", func() {
				// 	So(errs, ShouldBeEmpty)
				// })

			}

			// Convey("exhaustively check for all permutations of columns (ignoring order)", func() {
			// 	cols := make([]ColumnSelector, 0, len(result.ColumnsMetadata))
			// 	for col := range result.ColumnsMetadata {
			// 		cols = append(cols, col)
			// 	}
			// 	slices.Sort(cols)
			// 	N := len(cols)

			// 	isBitSet := func(v uint64, i uint) bool {
			// 		if i > 64 {
			// 			panic("must max be 64")
			// 		}
			// 		return (v & (1 << i)) != 0
			// 	}

			// 	alreadyTried := set.New[string]() // set of columns already tried

			// 	// run through all permutations. Whether a column is included is determined by whether the mask have the bit set matching the index in `cols`
			// 	for mask := uint64(1); mask < (1 << N); mask++ {
			// 		activeCols := make([]ColumnSelector, 0)
			// 		for k := range len(cols) {
			// 			if isBitSet(mask, uint(k)) {
			// 				activeCols = append(activeCols, cols[k])
			// 			}
			// 		}

			// 		s := fmt.Sprintf("%v", activeCols)
			// 		if alreadyTried.Contains(s) {
			// 			So(fmt.Errorf("already tried permutation of %s", s), ShouldBeNil)
			// 		}
			// 		alreadyTried.Add(s)

			// 		_, _, err := api.Query(ctx, db, result.TablesMetadata, Query{Select: activeCols, From: baseTable, Limit: 5})
			// 		if err != nil {
			// 			Convey(fmt.Sprintf("permutation with active cols %v", activeCols), func() {
			// 				So(err, ShouldBeNil)
			// 			})
			// 			break
			// 		}
			// 	}
			// })

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

func shouldResembleTablesMetadata(actual any, expected ...any) string {
	ats, ok := actual.(TablesMetadata)
	if !ok {
		return "actual should be of type TablesMetadata"
	}
	if len(expected) != 1 {
		return "'expected' should have length 1"
	}
	ets, ok := expected[0].(TablesMetadata)
	if !ok {
		return "expected[0] should be of type TablesMetadata"
	}

	s := ShouldResemble(getMapKeys(ats), getMapKeys(ets))
	if s != "" {
		return s
	}

	return ""
}

func sortedSlice[T ~string](xs []T) []T {
	slices.Sort(xs)
	return xs
}
