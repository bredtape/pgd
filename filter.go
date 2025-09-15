package pgd

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/bredtape/set"
	"github.com/pkg/errors"
)

type FilterOperator string

// FilterOperations is the supported 'where' operations from name to func(column, value) -> (sq.Sqlizer, error)
// The column is the quoted column name, but may have some prefix (uses ColumnSelectorFull.StringQuoted())
type FilterOperations map[DataType]map[FilterOperator](func(column string, value any) (sq.Sqlizer, error))

var (
	EqualsFilterOperations = map[FilterOperator]func(column string, value any) (sq.Sqlizer, error){
		"equals": func(c string, value any) (sq.Sqlizer, error) {
			return sq.Eq{c: value}, nil
		},
		"notEquals": func(c string, value any) (sq.Sqlizer, error) {
			return sq.NotEq{c: value}, nil
		},
	}
	// compare filter operations. Always false when comparing to null
	CompareFilterOperations = map[FilterOperator]func(column string, value any) (sq.Sqlizer, error){
		"greater": func(c string, value any) (sq.Sqlizer, error) {
			return sq.And{sq.NotEq{c: nil}, sq.Gt{c: value}}, nil
		},
		"greaterOrEquals": func(c string, value any) (sq.Sqlizer, error) {
			return sq.And{sq.NotEq{c: nil}, sq.GtOrEq{c: value}}, nil
		},
		"less": func(c string, value any) (sq.Sqlizer, error) {
			return sq.And{sq.NotEq{c: nil}, sq.Lt{c: value}}, nil
		},
		"lessOrEquals": func(c string, value any) (sq.Sqlizer, error) {
			return sq.And{sq.NotEq{c: nil}, sq.LtOrEq{c: value}}, nil
		},
	}
	NumberZeroFilterOperations = map[FilterOperator]func(column string, value any) (sq.Sqlizer, error){
		"isSpecified": func(c string, value any) (sq.Sqlizer, error) {
			return sq.And{sq.NotEq{c: nil}, sq.NotEq{c: 0}}, nil
		},
		"isNotSpecified": func(c string, value any) (sq.Sqlizer, error) {
			return sq.Or{sq.Eq{c: nil}, sq.Eq{c: 0}}, nil
		},
	}
	TextFilterOperations = map[FilterOperator]func(column string, value any) (sq.Sqlizer, error){
		"contains": func(c string, v any) (sq.Sqlizer, error) {
			s, ok := (v).(string)
			if !ok {
				return nil, errors.New("only supported for string")
			}
			return sq.And{sq.NotEq{c: nil}, sq.ILike{c: "%" + s + "%"}}, nil
		},
		"endsWith": func(c string, v any) (sq.Sqlizer, error) {
			s, ok := (v).(string)
			if !ok {
				return nil, errors.New("only supported for string")
			}
			return sq.And{sq.NotEq{c: nil}, sq.ILike{c: "%" + s}}, nil
		},
		"notContains": func(c string, v any) (sq.Sqlizer, error) {
			s, ok := (v).(string)
			if !ok {
				return nil, errors.New("only supported for string")
			}
			return sq.Or{sq.Eq{c: nil}, sq.NotILike{c: "%" + s + "%"}}, nil
		},
		"isNotSpecified": func(c string, v any) (sq.Sqlizer, error) {
			return sq.Or{sq.Eq{c: nil}, sq.Eq{c: ""}}, nil
		},
		"isSpecified": func(c string, v any) (sq.Sqlizer, error) {
			return sq.And{sq.NotEq{c: nil}, sq.NotEq{c: ""}}, nil
		},
		"startsWith": func(c string, v any) (sq.Sqlizer, error) {
			s, ok := (v).(string)
			if !ok {
				return nil, errors.New("only supported for string")
			}
			return sq.And{sq.NotEq{c: nil}, sq.ILike{c: s + "%"}}, nil
		},
	}
	TimestampFilterOperations = map[FilterOperator]func(column string, value any) (sq.Sqlizer, error){
		"after": func(c string, v any) (sq.Sqlizer, error) {
			return sq.And{sq.NotEq{c: nil}, sq.Gt{c: v}}, nil
		},
		"before": func(c string, v any) (sq.Sqlizer, error) {
			return sq.And{sq.NotEq{c: nil}, sq.Lt{c: v}}, nil
		},
		"isNotSpecified": func(c string, v any) (sq.Sqlizer, error) {
			return sq.Eq{c: nil}, nil
		},
		// there is no "empty" value for timestamp
		"isSpecified": func(c string, v any) (sq.Sqlizer, error) {
			return sq.NotEq{c: nil}, nil
		},
	}

	ArrayFilterOperations = map[FilterOperator]func(column string, value any) (sq.Sqlizer, error){
		"containsElement": func(c string, v any) (sq.Sqlizer, error) {
			return sq.And{sq.NotEq{c: nil}, sq.Expr(fmt.Sprintf("? = ANY (%s)", c), v)}, nil
		},
		"hasAnyElement": func(c string, v any) (sq.Sqlizer, error) {
			return sq.And{sq.NotEq{c: nil}, sq.Expr(fmt.Sprintf("CARDINALITY(%s)>0", c), v)}, nil
		},
		"hasNoElements": func(c string, v any) (sq.Sqlizer, error) {
			return sq.Or{sq.Eq{c: nil}, sq.Expr(fmt.Sprintf("CARDINALITY(%s)=0", c), v)}, nil
		},
		"notContainsElement": func(c string, v any) (sq.Sqlizer, error) {
			return sq.Or{sq.Eq{c: nil}, sq.Expr(fmt.Sprintf("NOT (? = ANY (%s))", c), v)}, nil
		},
	}

	numberOps               = MergeUniqueMaps(EqualsFilterOperations, CompareFilterOperations, NumberZeroFilterOperations)
	DefaultFilterOperations = FilterOperations{
		"bigint":                      numberOps,
		"double precision":            numberOps,
		"integer":                     numberOps,
		"real":                        numberOps,
		"text":                        MergeUniqueMaps(EqualsFilterOperations, TextFilterOperations),
		"text[]":                      MergeUniqueMaps(ArrayFilterOperations),
		"timestamp without time zone": TimestampFilterOperations,
		"uuid":                        EqualsFilterOperations,
	}
)

func (expr *WhereExpression) toSQL(filterOps FilterOperations, tables TablesMetadata, baseTable Table) (sq.Sqlizer, set.Set[ColumnSelectorFull], error) {
	// TODO: create more efficient lookup for ColumnMetadata (to get data type)
	colSelectors, err := tables.FlattenColumns(baseTable)
	if err != nil {
		return nil, nil, err
	}

	if expr.Filter != nil {
		f := *expr.Filter
		dt := colSelectors[f.Column].DataType
		op, exists := filterOps[dt][f.Operator]
		if !exists {
			return nil, nil, fmt.Errorf("unsupported filter operation: %s", f.Operator)
		}

		cbs, err := tables.ConvertColumnSelectors(baseTable, f.Column)
		if err != nil {
			return nil, nil, err
		}
		cb := cbs[0]

		cols := set.NewValues(cb)

		x, err := op(cb.StringQuoted(), f.Value)
		if err != nil {
			return nil, nil, err
		}
		return x, cols, nil
	}

	if len(expr.And) > 0 {
		var conj sq.And
		cols := set.New[ColumnSelectorFull](len(expr.And))
		for _, e := range expr.And {
			p, cs, err := e.toSQL(filterOps, tables, baseTable)
			if err != nil {
				return nil, nil, err
			}
			conj = append(conj, p)
			cols.AddSets(cs)
		}
		return conj, cols, nil
	}

	if len(expr.Or) > 0 {
		var conj sq.Or
		cols := set.New[ColumnSelectorFull](len(expr.Or))
		for _, e := range expr.Or {
			p, cs, err := e.toSQL(filterOps, tables, baseTable)
			if err != nil {
				return nil, nil, err
			}
			conj = append(conj, p)
			cols.AddSets(cs)
		}
		return conj, cols, nil
	}

	return nil, nil, fmt.Errorf("invalid where expression")
}

// WhereExpression represents a where/filter expression
// Must have exactly one of And, Or, Not or Filter set.
type WhereExpression struct {
	And    []WhereExpression `json:"and"`
	Or     []WhereExpression `json:"or"`
	Filter *Filter           `json:"filter"`
}

func (f WhereExpression) Validate() error {
	if err := f.validateWithParent(""); err != nil {
		return errors.Wrap(err, "invalid where expression")
	}
	return nil
}

func (f WhereExpression) validateWithParent(parent string) error {
	active := 0
	if f.Filter != nil {
		if err := f.Filter.Validate(); err != nil {
			return err
		}
		active++
	}

	if len(f.And) > 0 {
		active++
		for idx, e := range f.And {
			if err := e.validateWithParent(parent + fmt.Sprintf(".and[%d]", idx)); err != nil {
				return err
			}
		}
	}

	if len(f.Or) > 0 {
		active++
		for idx, e := range f.Or {
			if err := e.validateWithParent(parent + fmt.Sprintf(".or[%d]", idx)); err != nil {
				return err
			}
		}
	}

	if active == 0 {
		return fmt.Errorf("missing expression at %s", parent)
	}
	if active > 1 {
		return fmt.Errorf("multiple expressions at %s", parent)
	}

	return nil
}

type Filter struct {
	Column   ColumnSelector `json:"column"`
	Operator FilterOperator `json:"operator"`
	Value    any            `json:"value"`
}

func (f Filter) Validate() error {
	if !f.Column.IsValid() {
		return fmt.Errorf("invalid column '%s'", f.Column)
	}
	if f.Operator == "" {
		return fmt.Errorf("missing operator")
	}
	return nil
}

func MergeUniqueMaps[M ~map[K]V, K comparable, V any](src ...M) M {
	merged := make(M)
	for _, m := range src {
		for k, v := range m {
			if _, exists := merged[k]; exists {
				panic(fmt.Sprintf("duplicate key '%s'", k))
			}
			merged[k] = v
		}
	}
	return merged
}
