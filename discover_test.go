package pgd

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	. "github.com/smartystreets/goconvey/convey"
)

// uses table names: table1, table2, table3
const (
	TEST_DATABASE_URL = "postgres://postgres:pass@localhost:5432/tests?sslmode=disable"
)

func TestDiscoverSimpleTable1(t *testing.T) {
	ctx := t.Context()

	c := Config{
		FilterOperations: DefaultFilterOperations,
		ColumnDefaults: map[DataType]ColumnBehavior{
			"integer": {
				AllowSorting:     true,
				AllowFiltering:   false,
				FilterOperations: []FilterOperator{"equals", "notEquals", "greater", "greaterOrEquals", "less", "lessOrEquals"},
			},
			"text": {
				AllowSorting:     false,
				AllowFiltering:   true,
				FilterOperations: []FilterOperator{"equals", "notEquals", "greater", "greaterOrEquals", "less", "lessOrEquals"},
			},
			"double precision": {
				AllowSorting:     false,
				AllowFiltering:   false,
				FilterOperations: []FilterOperator{"equals"},
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

	schema := `
DROP TABLE IF EXISTS table1;
CREATE TABLE table1 (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  age DOUBLE PRECISION,
  description TEXT
);

COMMENT ON TABLE table1 IS E'{"properties": {"kk": "vv"}}';
COMMENT ON COLUMN table1.id IS E'{"properties": {"key1": "value1", "key2": "value2"}}';
COMMENT ON COLUMN table1.name IS E'{"properties": {"key3": "value3"},"filterOperations": ["contains", "notContains"]}';
COMMENT ON COLUMN table1.age IS E'{"properties": {"key4": "value4"}, "description": "age desc", "allowSorting": true, "allowFiltering": true, "filterOperations": ["equals", "notEquals"]}';
`

	expected := TableMetadata{
		Name: "table1",
		Behavior: TableBehavior{
			Properties: map[string]string{"kk": "vv"}},
		Columns: map[Column]ColumnMetadata{
			"id": {
				Name:       "id",
				DataType:   "integer",
				IsNullable: false,
				Behavior: ColumnBehavior{
					Properties:       map[string]string{"key1": "value1", "key2": "value2"},
					AllowSorting:     true,
					AllowFiltering:   false,
					FilterOperations: nil},
			},
			"name": {
				Name:       "name",
				DataType:   "text",
				IsNullable: false,
				Behavior: ColumnBehavior{
					Properties:       map[string]string{"key3": "value3"},
					AllowSorting:     false,
					AllowFiltering:   true,
					FilterOperations: []FilterOperator{"contains", "equals", "greater", "greaterOrEquals", "less", "lessOrEquals", "notContains", "notEquals"}},
			},
			"age": {
				Name:       "age",
				DataType:   "double precision",
				IsNullable: true,
				Behavior: ColumnBehavior{
					Properties:       map[string]string{"key4": "value4"},
					AllowSorting:     true,
					AllowFiltering:   true,
					FilterOperations: []FilterOperator{"equals", "notEquals"}},
			},
			"description": { // no comment on this column. Should have default behavior
				Name:       "description",
				DataType:   "text",
				IsNullable: true,
				Behavior: ColumnBehavior{
					Properties:       nil,
					AllowSorting:     false,
					AllowFiltering:   true,
					FilterOperations: []FilterOperator{"equals", "notEquals", "greater", "greaterOrEquals", "less", "lessOrEquals"}},
			},
		}}

	Convey("Given schema", t, func() {
		_, err = db.Exec(ctx, schema)
		So(err, ShouldBeNil)

		Convey("Discover table1", func() {
			result, err := api.Discover(ctx, db, "table1")
			So(err, ShouldBeNil)

			Convey("should have table metadata", func() {
				So(result.TablesMetadata, ShouldHaveLength, 1)
				So(result.TablesMetadata["table1"], ShouldResemble, expected)
			})
		})
	})
}

func TestDiscoverTableWithRelation(t *testing.T) {
	ctx := t.Context()

	defaultFilterOperations := []FilterOperator{"equals", "notEquals"}

	c := Config{
		FilterOperations: DefaultFilterOperations,
		ColumnDefaults: map[DataType]ColumnBehavior{
			"integer": {
				AllowSorting:     true,
				AllowFiltering:   true,
				FilterOperations: defaultFilterOperations},
			"text": {
				AllowSorting:     false,
				AllowFiltering:   true,
				FilterOperations: defaultFilterOperations}}}

	api, err := NewAPI(c)
	if err != nil {
		t.Fatalf("Failed to create API: %v", err)
	}
	db, err := getTestDB(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}
	defer db.Close(ctx)
	schema := `
DROP TABLE IF EXISTS table2;
DROP TABLE IF EXISTS table3;

CREATE TABLE table3 (
  other_id SERIAL PRIMARY KEY,
  other_name TEXT NOT NULL
);

CREATE TABLE table2 (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  other INTEGER REFERENCES table3(other_id)
);
`

	expected := TablesMetadata{
		"table2": TableMetadata{
			Name: "table2",
			Columns: map[Column]ColumnMetadata{
				"id": {
					Name:       "id",
					DataType:   "integer",
					IsNullable: false,
					Behavior: ColumnBehavior{
						AllowSorting:     true,
						AllowFiltering:   true,
						FilterOperations: []FilterOperator{"equals", "notEquals"}},
				},
				"name": {
					Name:       "name",
					DataType:   "text",
					IsNullable: false,
					Behavior: ColumnBehavior{
						AllowSorting:     false,
						AllowFiltering:   true,
						FilterOperations: []FilterOperator{"equals", "notEquals"}},
				},
				"other": {
					Name:       "other",
					DataType:   "integer",
					IsNullable: true,
					Behavior: ColumnBehavior{
						AllowSorting:     true,
						AllowFiltering:   true,
						FilterOperations: []FilterOperator{"equals", "notEquals"}},
					Relation: &ColumnRelation{
						Table:  "table3",
						Column: "other_id"},
				},
			}},
		"table3": TableMetadata{
			Name: "table3",
			Columns: map[Column]ColumnMetadata{
				"other_id": {
					Name:     "other_id",
					DataType: "integer",
					Behavior: ColumnBehavior{
						AllowSorting:     true,
						AllowFiltering:   true,
						FilterOperations: []FilterOperator{"equals", "notEquals"}},
				},
				"other_name": {
					Name:     "other_name",
					DataType: "text",
					Behavior: ColumnBehavior{
						AllowSorting:     false,
						AllowFiltering:   true,
						FilterOperations: []FilterOperator{"equals", "notEquals"}},
				},
			},
		},
	}

	Convey("Given schema", t, func() {
		_, err = db.Exec(ctx, schema)
		So(err, ShouldBeNil)

		Convey("Discover table2", func() {
			result, err := api.Discover(ctx, db, "table2")
			So(err, ShouldBeNil)

			Convey("base table should match", func() {
				So(result.BaseTable, ShouldEqual, Table("table2"))
			})

			Convey("expected result should also be valid", func() {
				err := expected.Validate()
				So(err, ShouldBeNil)
			})

			Convey("should have table metadata", func() {
				So(result.TablesMetadata, ShouldHaveLength, 2)
				So(result.TablesMetadata, ShouldResemble, expected)
			})
		})
	})
}

func getTestDB(ctx context.Context) (*pgx.Conn, error) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		url = TEST_DATABASE_URL
	}
	db, err := pgx.Connect(ctx, url)
	if err != nil {
		return nil, err
	}

	return db, nil
}
