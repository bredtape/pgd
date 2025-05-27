package pgd

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/bredtape/set"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

var (
	tableNameRegex = regexp.MustCompile(`^[a-z][a-zA-Z0-9_]{1,63}$`)

	// Regular expression for valid PostgreSQL column names
	columnNameRegex = regexp.MustCompile(`^[a-z][a-zA-Z0-9_]{1,63}$`)
)

type Table string

func (t Table) String() string {
	return string(t)
}

func (t Table) IsValid() bool {
	return tableNameRegex.MatchString(string(t))
}

func (t Table) StringQuoted() string {
	return fmt.Sprintf(`"%s"`, t)
}

type Column string

func (c Column) String() string {
	return string(c)
}

func (c Column) IsValid() bool {
	return columnNameRegex.MatchString(string(c))
}

// column selector (without base table), which may consists of <column>.
// When a (foreign) releation is used, the format is:
//
//	<column>.<foreign table>.<foreign column>
//
// but that may be nested.
type ColumnSelector string

func (cs ColumnSelector) String() string {
	return string(cs)
}

func (cs ColumnSelector) IsValid() bool {
	if cs.String() == "" {
		return false
	}
	return cs.WithBase("base").IsValid()
}

func (cs ColumnSelector) WithBase(t Table) columnSelectorBase {
	return columnSelectorBase(t.String() + "." + cs.String())
}

// column selector with base table, which may consists of <table>.<column>.
// When a (foreign) releation is used, the format is:
//
//	<base table>.<column>.<foreign table>.<foreign column>
//
// but that may be nested.
type columnSelectorBase string

func (cs columnSelectorBase) String() string {
	return string(cs)
}

func (cs columnSelectorBase) StringLower() string {
	return strings.ToLower(string(cs))
}

// to string with quoted prefix and column
func (cs columnSelectorBase) StringQuoted() string {
	prefix, c := cs.SplitAtLastColumn()
	return fmt.Sprintf(`"%s"."%s"`, prefix, c)
}

func (cs columnSelectorBase) IsValid() bool {
	if cs.String() == "" {
		return false
	}
	count := strings.Count(string(cs), ".")
	if count%2 != 1 {
		return false
	}
	tables, columns := cs.Breakdown()
	for _, t := range tables {
		if !t.IsValid() {
			return false
		}
	}
	for _, c := range columns {
		if !c.IsValid() {
			return false
		}
	}

	return true
}

func (cs columnSelectorBase) SplitAtLastColumn() (string, string) {
	idx := strings.LastIndex(string(cs), ".")
	return string(cs)[:idx], string(cs)[idx+1:]
}

func (cs columnSelectorBase) GetLastTable() Table {
	ts, _ := cs.Breakdown()
	return ts[len(ts)-1]
}

func (cs columnSelectorBase) ReplaceLastColumn(c Column) columnSelectorBase {
	idx := strings.LastIndex(string(cs), ".")
	return columnSelectorBase(string(cs)[:idx] + "." + c.String())
}

// get base table. Assumes that the column selector is valid
func (c columnSelectorBase) GetBasetable() Table {
	xs := strings.Split(string(c), ".")
	return Table(xs[0])
}

// breakdown column selector into table and column, where the same
// index are for the same table pair.
// Assumes that the column selector is valid
func (c columnSelectorBase) Breakdown() ([]Table, []Column) {
	xs := strings.Split(string(c), ".")
	tables := make([]Table, 0, len(xs)/2)
	columns := make([]Column, 0, len(xs)/2)
	for i := 0; i < len(xs); i += 2 {
		tables = append(tables, Table(xs[i]))
		columns = append(columns, Column(xs[i+1]))
	}
	return tables, columns
}

func ColumnSelectorRebuild(tables []Table, columns []Column) columnSelectorBase {
	if len(tables) != len(columns) {
		panic(fmt.Sprintf("invalid column selector: %v %v", tables, columns))
	}
	xs := make([]string, 0, len(tables)*2)
	for i := range tables {
		xs = append(xs, string(tables[i]), string(columns[i]))
	}
	return columnSelectorBase(strings.Join(xs, "."))
}

type OrderByExpression struct {
	ColumnSelector columnSelectorBase `json:"column"`
	IsDescending   bool               `json:"isDescending"`
}

type Query struct {
	Select  []ColumnSelector    `json:"select"`
	From    Table               `json:"from"`
	Where   *WhereExpression    `json:"where"`
	OrderBy []OrderByExpression `json:"orderBy"`
	Limit   uint64              `json:"limit"`
	Offset  uint64              `json:"offset"`
}

type QueryResult struct {
	Data  []map[string]any `json:"data"`  // data returned from the query by column name
	Limit uint64           `json:"limit"` // actual limit
	Total uint64           `json:"total"` // total number of rows matching the query
}

func (q Query) Validate() error {
	if len(q.Select) == 0 {
		return fmt.Errorf("missing select")
	}
	if !q.From.IsValid() {
		return fmt.Errorf("invalid from: %s", q.From)
	}
	if q.Where != nil {
		if err := q.Where.Validate(); err != nil {
			return errors.Wrap(err, "invalid filter expression")
		}
	}
	if q.Limit < 1 {
		return fmt.Errorf("invalid limit: %d", q.Limit)
	}
	return nil
}

type QueryDebug struct {
	PageSQL   string
	PageArgs  []any
	TotalSQL  string
	TotalArgs []any
}

func (qd QueryDebug) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("pageSQL", qd.PageSQL),
		slog.Any("pageArgs", qd.PageArgs),
		slog.String("totalSQL", qd.TotalSQL),
		slog.Any("totalArgs", qd.TotalArgs),
	)
}

func (api *API) Query(ctx context.Context, db *pgx.Conn, tables TablesMetadata, query Query) (QueryResult, QueryDebug, error) {
	debug := QueryDebug{}
	if err := query.Validate(); err != nil {
		return QueryResult{}, debug, errors.Wrap(err, "invalid query")
	}

	qPage, qTotal, err := api.convertQuery(tables, query)
	if err != nil {
		return QueryResult{}, debug, errors.Wrap(err, "invalid query")
	}

	batch := &pgx.Batch{}
	sqlTotal, argsTotal, err := qTotal.ToSql()
	if err != nil {
		return QueryResult{}, debug, errors.Wrap(err, "invalid (total) query")
	}
	batch.Queue(sqlTotal, argsTotal...)

	sqlPage, argsPage, err := qPage.ToSql()
	if err != nil {
		return QueryResult{}, debug, errors.Wrap(err, "invalid query")
	}
	batch.Queue(sqlPage, argsPage...)
	debug = QueryDebug{
		PageSQL:   sqlPage,
		PageArgs:  argsPage,
		TotalSQL:  sqlTotal,
		TotalArgs: argsTotal}

	tx, err := db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return QueryResult{}, debug, errors.Wrap(err, "failed to begin transaction")
	}
	defer tx.Commit(ctx)
	batchResults := tx.SendBatch(ctx, batch)
	defer batchResults.Close()

	var total uint64
	if err := batchResults.QueryRow().Scan(&total); err != nil {
		return QueryResult{}, debug, errors.Wrap(err, "failed to get total")
	}
	result := QueryResult{
		Data:  make([]map[string]any, 0),
		Limit: query.Limit,
		Total: total,
	}
	rows, err := batchResults.Query()
	if err != nil {
		return QueryResult{}, debug, errors.Wrap(err, "failed to get rows")
	}
	defer rows.Close()

	for rows.Next() {
		xs, err := rows.Values()
		if err != nil {
			return QueryResult{}, debug, errors.Wrap(err, "failed to scan row")
		}

		row := make(map[string]any, len(xs))
		for i := range rows.FieldDescriptions() {
			name := query.Select[i].String()
			row[name] = xs[i]
		}
		result.Data = append(result.Data, row)
	}

	if err := rows.Err(); err != nil {
		return QueryResult{}, debug, errors.Wrap(err, "error in rows")
	}

	return result, debug, nil
}

var (
	emptySelect = sq.SelectBuilder{}
)

// convert query to SQL given the tables metadata.
// Input args must be valid
func (api *API) convertQuery(tables TablesMetadata, query Query) (qPage sq.SelectBuilder, qTotal sq.SelectBuilder, err error) {
	columnsUsed := set.New[columnSelectorBase](len(query.Select))
	cols := make([]string, 0, len(query.Select))
	for _, c := range query.Select {
		cb := c.WithBase(query.From)
		if !cb.IsValid() {
			return emptySelect, emptySelect, fmt.Errorf("invalid column selector: %s", c)
		}

		columnsUsed.Add(cb)
		cols = append(cols, cb.StringQuoted())
	}

	qPage = sq.
		Select(cols...).
		From(query.From.StringQuoted()).
		Limit(query.Limit).
		Offset(query.Offset).
		PlaceholderFormat(sq.Dollar)

	qTotal = sq.
		Select("count(*)").
		From(query.From.StringQuoted()).
		PlaceholderFormat(sq.Dollar)

	if query.Where != nil {
		qf, cs, err := query.Where.toSql(query.From)
		if err != nil {
			return emptySelect, emptySelect, errors.Wrap(err, "invalid filter expression")
		}

		if !cs.IsSubsetOf(columnsUsed) {
			less, _, _ := cs.Diff(columnsUsed)
			return emptySelect, emptySelect, fmt.Errorf("invalid filter expression, some columns were used in where filter expression, but not in select: %s", less.String())
		}
		qPage = qPage.Where(qf)
		qTotal = qTotal.Where(qf)
	}

	joins, err := processJoins(tables, columnsUsed)
	if err != nil {
		return emptySelect, emptySelect, errors.Wrap(err, "invalid foreign relations")
	}
	for _, j := range joins {
		toPrefix, _ := j.To.SplitAtLastColumn()
		joinExpr := fmt.Sprintf(`"%s" AS "%s" ON %s = %s`,
			j.To.GetLastTable(), toPrefix, j.From.StringQuoted(), j.To.StringQuoted())
		if j.IsOuterJoin {
			qPage = qPage.LeftJoin(joinExpr)
			qTotal = qTotal.LeftJoin(joinExpr)
		} else {
			qPage = qPage.InnerJoin(joinExpr)
			qTotal = qTotal.InnerJoin(joinExpr)
		}
	}

	for _, c := range query.OrderBy {
		if _, ok := columnsUsed[c.ColumnSelector]; !ok {
			return emptySelect, emptySelect, fmt.Errorf("invalid order by column selector %s, not used in select", c.ColumnSelector.String())
		}

		if c.IsDescending {
			qPage = qPage.OrderBy(c.ColumnSelector.StringQuoted() + " DESC")
		} else {
			qPage = qPage.OrderBy(c.ColumnSelector.StringQuoted())
		}
	}

	return qPage, qTotal, nil
}

type TableColumn struct {
	Table  Table
	Column Column
}

type Join struct {
	IsOuterJoin bool
	From        columnSelectorBase
	To          columnSelectorBase
}

// process foreign relations
func processJoins(tables TablesMetadata, columnsUsed set.Set[columnSelectorBase]) ([]Join, error) {
	result := make([]Join, 0, len(columnsUsed))

	alreadyJoined := set.New[string](0)
	for c := range columnsUsed {
		ts, cols := c.Breakdown()

		if len(ts) == 1 {
			continue
		}

		for i := range len(ts) - 1 {
			source := ColumnSelectorRebuild(ts[:i+1], cols[:i+1])
			target := ColumnSelectorRebuild(ts[:i+2], cols[:i+2])
			prefix, _ := target.SplitAtLastColumn()
			if alreadyJoined.Contains(prefix) {
				continue
			}
			alreadyJoined.Add(prefix)

			sourceTable, exists := tables[ts[i]]
			if !exists {
				return nil, fmt.Errorf("invalid (source) table %s", ts[i])
			}
			sourceCol, exists := sourceTable.Columns[cols[i]]
			if !exists {
				return nil, fmt.Errorf("invalid (source) column '%s' in table '%s'", cols[i], sourceTable.Name)
			}
			targetTable, exists := tables[ts[i+1]]
			if !exists {
				return nil, fmt.Errorf("invalid foreign table '%s'", ts[i+1])
			}
			if sourceCol.Relation == nil {
				return nil, fmt.Errorf("invalid foreign column '%s', no relation", sourceCol.Name)
			}
			if sourceCol.Relation.Table != targetTable.Name {
				return nil, fmt.Errorf("invalid foreign column '%s', foreign table '%s' does not match '%s'", sourceCol.Name, sourceCol.Relation.Table, targetTable.Name)
			}

			result = append(result, Join{
				IsOuterJoin: sourceCol.IsNullable,
				From:        source,
				To:          target.ReplaceLastColumn(sourceCol.Relation.Column)})
		}
	}
	return result, nil
}
