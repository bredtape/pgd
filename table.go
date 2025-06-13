package pgd

import (
	"fmt"

	"github.com/pkg/errors"
)

// table metadata
type TableMetadata struct {
	Name Table `json:"name"`

	// columns by name
	Columns  map[Column]ColumnMetadata `json:"columns"`
	Behavior TableBehavior             `json:"behavior"`
}

func (t TableMetadata) Validate() error {
	if t.Name == "" {
		return fmt.Errorf("missing table name")
	}
	if !t.Name.IsValid() {
		return fmt.Errorf("invalid table name")
	}
	if len(t.Columns) == 0 {
		return fmt.Errorf("missing columns")
	}
	for ck, c := range t.Columns {
		if err := c.Validate(); err != nil {
			return errors.Wrapf(err, "invalid column %s", c.Name)
		}
		if ck != c.Name {
			return fmt.Errorf("column name %s does not match key %s", c.Name, ck)
		}
	}
	return nil
}

// metadata for all tables
type TablesMetadata map[Table]TableMetadata

func (ts TablesMetadata) Validate() error {
	for tk, t := range ts {
		if err := t.Validate(); err != nil {
			return errors.Wrapf(err, "invalid table %s", t.Name)
		}
		if tk != t.Name {
			return fmt.Errorf("table name %s does not match key %s", t.Name, tk)
		}

		// validate all column relations
		for _, c := range t.Columns {
			if c.Relation != nil {
				// check if the foreign table exists
				foreignTable, ok := ts[c.Relation.Table]
				if !ok {
					return fmt.Errorf("invalid foreign table %s for column %s in table %s", c.Relation.Table, c.Name, t.Name)
				}
				// check if the foreign column exists
				foreignColumn, ok := foreignTable.Columns[c.Relation.Column]
				if !ok {
					return fmt.Errorf("invalid foreign column %s for column %s in table %s", c.Relation.Column, c.Name, t.Name)
				}

				if c.DataType != foreignColumn.DataType {
					return fmt.Errorf("invalid foreign column %s for column %s in table %s, data type %s does not match %s", c.Relation.Column, c.Name, t.Name, c.DataType, foreignColumn.DataType)
				}
			}
		}
	}
	return nil
}

func (ts TablesMetadata) FlattenColumns(baseTable Table) (map[ColumnSelector]ColumnMetadata, error) {
	result := make(map[ColumnSelector]ColumnMetadata)

	err := ts.flattenColumns(result, nil, baseTable)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (ts TablesMetadata) flattenColumns(result map[ColumnSelector]ColumnMetadata, parents []Column, table Table) error {

	tableMeta, exists := ts[table]
	if !exists {
		return fmt.Errorf("table '%s' not found (via relation %v)", table, parents)
	}

	// walk BFS
	for column, colMeta := range tableMeta.Columns {
		cols := append(parents, column)
		c := NewColumnSelector(cols...)
		result[c] = colMeta

		if colMeta.Relation != nil {
			err := ts.flattenColumns(result, cols, colMeta.Relation.Table)
			if err != nil {
				return errors.Wrapf(err, "failed to flatten table '%s', column '%s' via relation %v", table, column, parents)
			}
		}
	}
	return nil
}

func (ts TablesMetadata) ConvertColumnSelectors(baseTable Table, css ...ColumnSelector) ([]ColumnSelectorFull, error) {
	result := make([]ColumnSelectorFull, 0, len(css))
	for _, c := range css {
		x, err := ts.ConvertColumnSelector(baseTable, c)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to convert column selector '%s'", c)
		}
		result = append(result, x)
	}
	return result, nil
}

// convert from column selector, e.g. "col1.col2.col3" to 'full' format with table information, e.g. "baseTable.col1.tableB.col2.tableC.col3"
func (ts TablesMetadata) ConvertColumnSelector(baseTable Table, cs ColumnSelector) (ColumnSelectorFull, error) {
	columns := cs.GetColumns()
	if len(columns) == 0 {
		return "", errors.New("invalid columns")
	}

	tables := []Table{baseTable} // extended on every iteration in the loop
	for i := range len(columns) {
		table := tables[i]
		t, exists := ts[table]
		if !exists {
			return "", fmt.Errorf("table %s not found in table metadata when building column selector for %s", table, cs)
		}

		column := columns[i]
		tc, exists := t.Columns[column]
		if !exists {
			return "", fmt.Errorf("table '%s' does not have column '%s'", table, column)
		}

		// not at the end, so there must be a relation
		if i < len(columns)-1 {
			if tc.Relation == nil {
				return "", fmt.Errorf("table %s, column %s should have some relation, but does not", table, column)
			}
			r := *tc.Relation
			tables = append(tables, r.Table)
		}
	}

	if len(tables) != len(columns) {
		return "", fmt.Errorf("internal error, there should be as many tables %v as there are columns %v", tables, columns)
	}

	return ColumnSelectorRebuild(tables, columns), nil
}

type TableBehavior struct {
	Properties map[string]string `json:"properties"`
}
