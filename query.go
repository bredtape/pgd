package pgd

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"

	sq "github.com/Masterminds/squirrel"
	"github.com/bredtape/set"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

var (
	tableNameRegex = regexp.MustCompile(`^[a-z][a-zA-Z0-9_]{1,63}$`)
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

type OrderByExpression struct {
	ColumnSelector ColumnSelector `json:"column"`
	IsDescending   bool           `json:"isDescending"`
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
	selectors, err := tables.ConvertColumnSelectors(query.From, query.Select...)
	if err != nil {
		return sq.SelectBuilder{}, sq.SelectBuilder{}, err
	}

	columnsUsed := set.New[ColumnSelectorFull](len(query.Select))
	cols := make([]string, 0, len(query.Select))
	for _, c := range selectors {
		columnsUsed.Add(c)
		cols = append(cols, c.StringQuoted())
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
		qf, cols, err := query.Where.toSQL(api.c.FilterOperations, tables, query.From)
		if err != nil {
			return emptySelect, emptySelect, errors.Wrap(err, "invalid filter expression")
		}
		columnsUsed.AddSets(cols)

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
		if j.UseLeftJoin {
			qPage = qPage.LeftJoin(joinExpr)
			qTotal = qTotal.LeftJoin(joinExpr)
		} else {
			qPage = qPage.InnerJoin(joinExpr)
			qTotal = qTotal.InnerJoin(joinExpr)
		}
	}

	for _, c := range query.OrderBy {
		cs, err := tables.ConvertColumnSelector(query.From, c.ColumnSelector)
		if err != nil {
			return qPage, qTotal, errors.Wrapf(err, "failed to convert column selector in orderby expression")
		}

		if _, ok := columnsUsed[cs]; !ok {
			return emptySelect, emptySelect, fmt.Errorf("invalid order by column selector %s, not used in select", cs.String())
		}

		suffix := ""
		if c.IsDescending {
			suffix = " DESC"
		}
		qPage = qPage.OrderBy(cs.StringQuoted() + suffix)
	}

	return qPage, qTotal, nil
}

type tableJoin struct {
	UseLeftJoin bool
	From        ColumnSelectorFull
	To          ColumnSelectorFull
}

// process foreign relations
func processJoins(tables TablesMetadata, columnsUsed set.Set[ColumnSelectorFull]) ([]tableJoin, error) {
	result := make([]tableJoin, 0, len(columnsUsed))

	alreadyJoined := set.New[string](0)
	for c := range columnsUsed {
		ts, cols := c.Breakdown()

		if len(ts) == 1 {
			continue
		}

		parentNull := false
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

			// if this or any previous relation is optional (NULL), we must use LEFT JOIN for all descendants
			parentNull = parentNull || sourceCol.IsNullable

			result = append(result, tableJoin{
				UseLeftJoin: parentNull,
				From:        source,
				To:          target.ReplaceLastColumn(sourceCol.Relation.Column)})
		}
	}
	return result, nil
}
