package pgd

import (
	"fmt"
	"slices"

	sq "github.com/Masterminds/squirrel"
	"github.com/bredtape/set"
	"github.com/pkg/errors"
)

type FilterOperator string

var (
	// supported 'where' operations from name to func(column, value) -> (sq.Sqlizer, error)
	filterOperations = map[FilterOperator](func(string, any) (sq.Sqlizer, error)){
		"contains": func(s string, v any) (sq.Sqlizer, error) {
			vs, ok := (v).(string)
			if !ok {
				return nil, errors.New("contains only supported for string")
			}
			return sq.Like{s: "%" + vs + "%"}, nil
		},
		"containsInsensitive": func(s string, v any) (sq.Sqlizer, error) {
			vs, ok := (v).(string)
			if !ok {
				return nil, errors.New("contains only supported for string")
			}
			return sq.ILike{s: "%" + vs + "%"}, nil
		},
		"endsWith": func(s string, v any) (sq.Sqlizer, error) {
			vs, ok := (v).(string)
			if !ok {
				return nil, errors.New("contains only supported for string")
			}
			return sq.Like{s: "%" + vs}, nil
		},
		"equal":          func(s string, v any) (sq.Sqlizer, error) { return sq.Eq{s: v}, nil },
		"greater":        func(s string, v any) (sq.Sqlizer, error) { return sq.Gt{s: v}, nil },
		"greaterOrEqual": func(s string, v any) (sq.Sqlizer, error) { return sq.GtOrEq{s: v}, nil },
		"less":           func(s string, v any) (sq.Sqlizer, error) { return sq.Lt{s: v}, nil },
		"lessOrEqual":    func(s string, v any) (sq.Sqlizer, error) { return sq.LtOrEq{s: v}, nil },
		"notContains": func(s string, v any) (sq.Sqlizer, error) {
			vs, ok := (v).(string)
			if !ok {
				return nil, errors.New("contains only supported for string")
			}
			return sq.NotLike{s: "%" + vs + "%"}, nil
		},
		"notContainsInsensitive": func(s string, v any) (sq.Sqlizer, error) {
			vs, ok := (v).(string)
			if !ok {
				return nil, errors.New("contains only supported for string")
			}
			return sq.NotILike{s: "%" + vs + "%"}, nil
		},
		"notEqual": func(s string, v any) (sq.Sqlizer, error) { return sq.NotEq{s: v}, nil },
		"startsWith": func(s string, v any) (sq.Sqlizer, error) {
			vs, ok := (v).(string)
			if !ok {
				return nil, errors.New("contains only supported for string")
			}
			return sq.Like{s: vs + "%"}, nil
		},
	}
)

func GetFilterOperations() []FilterOperator {
	keys := make([]FilterOperator, 0, len(filterOperations))
	for k := range filterOperations {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func (expr *WhereExpression) toSql() (sq.Sqlizer, set.Set[ColumnSelector], error) {
	return expr.toSqlChild2()
}

func (expr WhereExpression) toSqlChild2() (sq.Sqlizer, set.Set[ColumnSelector], error) {

	if expr.Filter != nil {
		f := *expr.Filter
		op, exists := filterOperations[f.Operator]
		if !exists {
			return nil, nil, fmt.Errorf("unsupported filter operation: %s", f.Operator)
		}
		cols := map[ColumnSelector]struct{}{expr.Filter.Column: {}}

		x, err := op(f.Column.StringQuoted(), f.Value)
		if err != nil {
			return nil, nil, err
		}
		return x, cols, nil
	}

	if len(expr.And) > 0 {
		var conj sq.And
		cols := set.New[ColumnSelector](len(expr.And))
		for _, e := range expr.And {
			p, cs, err := e.toSqlChild2()
			if err != nil {
				return nil, nil, err
			}
			conj = append(conj, p)
			for k := range cs {
				cols[k] = struct{}{}
			}
		}
		return conj, cols, nil
	}

	if len(expr.Or) > 0 {
		var conj sq.Or
		cols := set.New[ColumnSelector](len(expr.Or))
		for _, e := range expr.Or {
			p, cs, err := e.toSqlChild2()
			if err != nil {
				return nil, nil, err
			}
			conj = append(conj, p)
			for k := range cs {
				cols[k] = struct{}{}
			}
		}
		return conj, cols, nil
	}

	return nil, nil, fmt.Errorf("invalid where expression")
}

// where/fitler expression
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
			return errors.Wrapf(err, "invalid where at %s", parent)
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
	if f.Column == "" {
		return fmt.Errorf("missing column")
	}
	if f.Operator == "" {
		return fmt.Errorf("missing operator")
	}
	return nil
}
