package pgd

import (
	"fmt"
	"reflect"

	sq "github.com/Masterminds/squirrel"
	"github.com/pkg/errors"
)

type FilterOperator string

// FilterOperations is the supported 'where' operations from name to func(column, value) -> (sq.Sqlizer, error)
// The column is the quoted column name, but may have some prefix (uses ColumnSelectorFull.StringQuoted())
type FilterOperations map[FilterOperator](func(string, any) (sq.Sqlizer, error))

var (
	DefaultFilterOperations = FilterOperations{
		"any": func(s string, v any) (sq.Sqlizer, error) {
			if v == nil {
				return nil, errors.New("argument is nil")
			}
			t := reflect.TypeOf(v)
			isSlice := t.Kind() == reflect.Slice
			if isSlice {
				return nil, fmt.Errorf("argument '%v' must not be a list", v)
			}
			return sq.Expr(fmt.Sprintf("? = ANY (%s)", s), v), nil
		},
		"contains": func(s string, v any) (sq.Sqlizer, error) {
			vs, ok := (v).(string)
			if !ok {
				return nil, errors.New("only supported for string")
			}
			return sq.Like{s: "%" + vs + "%"}, nil
		},
		"containsInsensitive": func(s string, v any) (sq.Sqlizer, error) {
			vs, ok := (v).(string)
			if !ok {
				return nil, errors.New("only supported for string")
			}
			return sq.ILike{s: "%" + vs + "%"}, nil
		},
		"endsWith": func(s string, v any) (sq.Sqlizer, error) {
			vs, ok := (v).(string)
			if !ok {
				return nil, errors.New("only supported for string")
			}
			return sq.Like{s: "%" + vs}, nil
		},
		"equals":          func(s string, v any) (sq.Sqlizer, error) { return sq.Eq{s: v}, nil },
		"greater":         func(s string, v any) (sq.Sqlizer, error) { return sq.Gt{s: v}, nil },
		"greaterOrEquals": func(s string, v any) (sq.Sqlizer, error) { return sq.GtOrEq{s: v}, nil },
		"less":            func(s string, v any) (sq.Sqlizer, error) { return sq.Lt{s: v}, nil },
		"lessOrEquals":    func(s string, v any) (sq.Sqlizer, error) { return sq.LtOrEq{s: v}, nil },
		"notContains": func(s string, v any) (sq.Sqlizer, error) {
			vs, ok := (v).(string)
			if !ok {
				return nil, errors.New("only supported for string")
			}
			return sq.NotLike{s: "%" + vs + "%"}, nil
		},
		"notContainsInsensitive": func(s string, v any) (sq.Sqlizer, error) {
			vs, ok := (v).(string)
			if !ok {
				return nil, errors.New("only supported for string")
			}
			return sq.NotILike{s: "%" + vs + "%"}, nil
		},
		"notEquals": func(s string, v any) (sq.Sqlizer, error) { return sq.NotEq{s: v}, nil },
		"startsWith": func(s string, v any) (sq.Sqlizer, error) {
			vs, ok := (v).(string)
			if !ok {
				return nil, errors.New("only supported for string")
			}
			return sq.Like{s: vs + "%"}, nil
		},
	}
)

func (expr *WhereExpression) toSql(filterOps FilterOperations, tables TablesMetadata, baseTable Table) (sq.Sqlizer, error) {

	if expr.Filter != nil {
		f := *expr.Filter
		op, exists := filterOps[f.Operator]
		if !exists {
			return nil, fmt.Errorf("unsupported filter operation: %s", f.Operator)
		}
		cbs, err := tables.ConvertColumnSelectors(baseTable, f.Column)
		if err != nil {
			return nil, err
		}
		cb := cbs[0]

		x, err := op(cb.StringQuoted(), f.Value)
		if err != nil {
			return nil, err
		}
		return x, nil
	}

	if len(expr.And) > 0 {
		var conj sq.And
		for _, e := range expr.And {
			p, err := e.toSql(filterOps, tables, baseTable)
			if err != nil {
				return nil, err
			}
			conj = append(conj, p)
		}
		return conj, nil
	}

	if len(expr.Or) > 0 {
		var conj sq.Or
		for _, e := range expr.Or {
			p, err := e.toSql(filterOps, tables, baseTable)
			if err != nil {
				return nil, err
			}
			conj = append(conj, p)
		}
		return conj, nil
	}

	if len(expr.Or) > 0 {
		var conj sq.Or
		for _, e := range expr.Or {
			p, err := e.toSql(filterOps, tables, baseTable)
			if err != nil {
				return nil, err
			}
			conj = append(conj, p)
		}
		return conj, nil
	}

	return nil, fmt.Errorf("invalid where expression")
}

// WhereExpression represents a where/filter expression
// Must have exactly one of And, Or, Not or Filter set.
type WhereExpression struct {
	And    []WhereExpression `json:"and"`
	Or     []WhereExpression `json:"or"`
	Filter *Filter           `json:"filter"`
}

func (f WhereExpression) Validate() error {
	if err := f.validate(""); err != nil {
		return errors.Wrap(err, "invalid where expression")
	}
	return nil
}

func (f WhereExpression) validate(parent string) error {
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
			if err := e.validate(parent + fmt.Sprintf(".and[%d]", idx)); err != nil {
				return err
			}
		}
	}

	if len(f.Or) > 0 {
		active++
		for idx, e := range f.Or {
			if err := e.validate(parent + fmt.Sprintf(".or[%d]", idx)); err != nil {
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
