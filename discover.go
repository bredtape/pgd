package pgd

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	sq "github.com/Masterminds/squirrel"
	"github.com/bredtape/set"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

const (
	defaultSchema = "public"
	defaultLimit  = 200
	maxLimit      = 1000
)

type API struct {
	c Config
}

func NewAPI(c Config) (*API, error) {
	if c.Schema == "" {
		c.Schema = defaultSchema
	}
	if c.DefaultLimit == 0 {
		c.DefaultLimit = defaultLimit
	}
	if ve := c.Validate(); ve != nil {
		return nil, errors.Wrap(ve, "invalid config")
	}
	return &API{c: c}, nil
}

type DiscoverResult struct {
	BaseTable       Table                             `json:"baseTable"`
	TablesMetadata  TablesMetadata                    `json:"tables"`  // metadata pr table
	ColumnsMetadata map[ColumnSelector]ColumnMetadata `json:"columns"` // map of all columns. Same content as TablesMetadata, but flattened
}

// Discover retrieves metadata for the base table and all related tables.
func (api *API) Discover(ctx context.Context, conn *pgx.Conn, baseTable Table) (DiscoverResult, error) {
	tables := make(TablesMetadata, 1)
	err := api.discoverWithRelations(ctx, conn, tables, baseTable)
	if err != nil {
		return DiscoverResult{}, err
	}

	// Validate the metadata
	if err := tables.Validate(); err != nil {
		return DiscoverResult{}, errors.Wrap(err, "invalid table metadata")
	}

	cols, err := tables.FlattenColumns(baseTable)
	if err != nil {
		return DiscoverResult{}, errors.Wrap(err, "failed to index metadata by columns")
	}

	result := DiscoverResult{
		BaseTable:       baseTable,
		TablesMetadata:  tables,
		ColumnsMetadata: cols}
	return result, nil
}

// discover base table and all related tables
func (api *API) discoverWithRelations(ctx context.Context, conn *pgx.Conn, known TablesMetadata, baseTable Table) error {

	// Get table metadata
	otherTables, err := api.discoverSingle(ctx, conn, known, baseTable)
	if err != nil {
		return errors.Wrap(err, "failed to discover table metadata")
	}

	for table := range otherTables {
		if _, exists := known[table]; !exists {
			err = api.discoverWithRelations(ctx, conn, known, table)
			if err != nil {
				return errors.Wrap(err, "failed to discover related table metadata")
			}
		}
	}

	return nil
}

// GetTableMetadata retrieves comprehensive metadata for a specified table using batch querying
func (api *API) discoverSingle(ctx context.Context, conn *pgx.Conn, known TablesMetadata, table Table) (set.Set[Table], error) {
	// Create a new batch
	batch := &pgx.Batch{}

	// Build SQL queries using squirrel
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	// Query 1: Get table information
	tableInfoQuery, tableInfoArgs, err := psql.
		Select("c.relname AS table_name", "pg_catalog.obj_description(c.oid, 'pg_class') AS table_comment").
		From("pg_catalog.pg_class c").
		Join("pg_catalog.pg_namespace n ON n.oid = c.relnamespace").
		Where(sq.Eq{
			"n.nspname": api.c.Schema,
			"c.relname": table,
			"c.relkind": "r", // r = regular table
		}).
		ToSql()
	if err != nil {
		return nil, errors.Wrap(err, "failed to build table info query")
	}
	batch.Queue(tableInfoQuery, tableInfoArgs...)

	// Query 2: Get column details
	columnsQuery, columnsArgs, err := psql.
		Select(
			"a.attname AS column_name",
			"pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type",
			"NOT a.attnotnull AS is_nullable",
			"pg_catalog.col_description(a.attrelid, a.attnum) AS column_comment",
		).
		From("pg_catalog.pg_attribute a").
		Join("pg_catalog.pg_class c ON c.oid = a.attrelid").
		Join("pg_catalog.pg_namespace n ON n.oid = c.relnamespace").
		Where(sq.And{
			sq.Eq{"n.nspname": api.c.Schema},
			sq.Eq{"c.relname": table.String()},
			sq.Gt{"a.attnum": 0},           // Skip system columns
			sq.Eq{"a.attisdropped": false}, // Skip dropped columns
		}).
		OrderBy("a.attnum").
		ToSql()
	if err != nil {
		return nil, errors.Wrap(err, "failed to build column details query")
	}
	batch.Queue(columnsQuery, columnsArgs...)

	// Query 3: Get foreign key references
	fkQuery, fkArgs, err := psql.
		Select(
			"kcu.column_name",
			"ccu.table_schema AS foreign_table_schema",
			"ccu.table_name AS foreign_table_name",
			"ccu.column_name AS foreign_column_name",
		).
		From("information_schema.table_constraints tc").
		Join("information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema").
		Join("information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema").
		Where(sq.And{
			sq.Eq{"tc.constraint_type": "FOREIGN KEY"},
			sq.Eq{"tc.table_schema": api.c.Schema},
			sq.Eq{"tc.table_name": table.String()},
		}).
		ToSql()
	if err != nil {
		return nil, errors.Wrap(err, "failed to build foreign keys query")
	}
	batch.Queue(fkQuery, fkArgs...)

	// Execute the batch
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, errors.Wrap(err, "failed to begin transaction")
	}
	defer tx.Commit(ctx)
	results := tx.SendBatch(ctx, batch)
	defer results.Close()

	// Process table info results
	tableInfo := TableMetadata{Columns: make(map[Column]ColumnMetadata)}
	var comment *string
	row := results.QueryRow()
	if err := row.Scan(&tableInfo.Name, &comment); err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("table %s.%s not found", api.c.Schema, table)
		}
		return nil, errors.Wrap(err, "failed to scan table info")
	}
	if comment != nil {
		var behavior TableBehavior
		err = json.Unmarshal([]byte(*comment), &behavior)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshal table %s comment as TableBehavior", table)
		}
		tableInfo.Behavior = behavior
	}

	// Process column details results
	rows, err := results.Query()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get column details")
	}
	defer rows.Close()

	for rows.Next() {
		var col ColumnMetadata
		var comment *string
		if err := rows.Scan(&col.Name, &col.DataType, &col.IsNullable, &comment); err != nil {
			return nil, errors.Wrap(err, "failed to scan column details")
		}
		b, err := api.parseAndMergeColumnBehavior(col.DataType, comment)
		if err != nil {
			var safeComment string
			if comment != nil {
				safeComment = *comment
			}
			return nil, errors.Wrapf(err, "failed to parse column behavior for column '%s', datatype '%s' with comment '%s'", col.Name, col.DataType, safeComment)
		}
		col.Behavior = b
		tableInfo.Columns[col.Name] = col
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating column rows")
	}

	// Process foreign keys results
	fkRows, err := results.Query()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get foreign key details")
	}
	defer fkRows.Close()

	otherTables := set.New[Table]()
	for fkRows.Next() {
		var fkSchema string
		var colName, fkColumn Column
		var fkTable Table
		if err := fkRows.Scan(&colName, &fkSchema, &fkTable, &fkColumn); err != nil {
			return nil, errors.Wrap(err, "failed to scan foreign key data")
		}

		// Only include references if they're in the same schema (assuming 1:1 relations)
		//if fkSchema == schemaName {
		col, exists := tableInfo.Columns[colName]
		if !exists {
			return nil, fmt.Errorf("column %s not found in table %s", colName, tableInfo.Name)
		}
		col.Relation = &ColumnRelation{
			Table:  fkTable,
			Column: fkColumn}
		tableInfo.Columns[colName] = col
		//}
		otherTables.Add(fkTable)
	}
	fkRows.Close()
	if err := fkRows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating foreign key rows")
	}

	known[tableInfo.Name] = tableInfo

	return otherTables, nil
}

func (api *API) parseAndMergeColumnBehavior(dataType DataType, raw *string) (ColumnBehavior, error) {
	d, exists := api.c.ColumnDefaults[dataType]
	if !exists {
		return d, fmt.Errorf("no column defaults for data type '%s'", dataType)
	}

	if raw == nil || *raw == "" {
		return d, nil
	}

	// Unmarshal the raw JSON string into a map to check whether optional keys are present
	var m map[string]any
	err := json.Unmarshal([]byte(*raw), &m)
	if err != nil {
		return ColumnBehavior{}, errors.Wrap(err, "failed to unmarshal column behavior")
	}

	var b ColumnBehavior
	if err := json.Unmarshal([]byte(*raw), &b); err != nil {
		return ColumnBehavior{}, errors.Wrap(err, "failed to unmarshal column behavior")
	}

	if _, exists := m["allowSorting"]; !exists {
		b.AllowSorting = d.AllowSorting
	}
	if _, exists := m["allowFiltering"]; !exists {
		b.AllowFiltering = d.AllowFiltering
	}
	if _, exists := m["omitDefaultFilterOperations"]; !exists {
		b.OmitDefaultFilterOperations = d.OmitDefaultFilterOperations
	}
	if _, exists := m["filterOperations"]; !exists {
		b.FilterOperations = d.FilterOperations
	}

	if !b.OmitDefaultFilterOperations {
		b.FilterOperations = append(b.FilterOperations, d.FilterOperations...)
	}

	b.FilterOperations = uniqueSliceString(b.FilterOperations)

	if !b.AllowFiltering {
		b.FilterOperations = nil
	}

	return b, nil
}

func uniqueSliceString[T ~string](xs []T) []T {
	seen := make(map[T]struct{})
	var result []T
	for _, x := range xs {
		if _, ok := seen[x]; !ok {
			seen[x] = struct{}{}
			result = append(result, x)
		}
	}
	slices.Sort(result)
	return result
}
