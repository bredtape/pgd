package pgd

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	columnNameRegex = regexp.MustCompile(`^[a-z][a-zA-Z0-9_]{1,63}$`)
)

// simple column
type Column string

func (c Column) String() string {
	return string(c)
}

func (c Column) IsValid() bool {
	return columnNameRegex.MatchString(string(c))
}

// column selector (without base table), may simple be <column> or
// when a foreign releation is used	<column>.<foreign table column>
// and so on
type ColumnSelector string

func (cs ColumnSelector) String() string {
	return string(cs)
}

func (cs ColumnSelector) IsValid() bool {
	xs := cs.GetColumns()
	if len(xs) == 0 {
		return false
	}
	for _, x := range xs {
		if !Column(x).IsValid() {
			return false
		}
	}
	return true
}

func (cs ColumnSelector) GetColumns() []Column {
	xs := strings.Split(string(cs), ".")
	result := make([]Column, 0, len(xs))
	for _, x := range xs {
		result = append(result, Column(x))
	}
	return result
}

func NewColumnSelector(cs ...Column) ColumnSelector {
	xs := make([]string, 0, len(cs))
	for _, c := range cs {
		xs = append(xs, c.String())
	}
	return ColumnSelector(strings.Join(xs, "."))
}

// column selector with table information.
// The format is<table>.<column> and may be nested with foreign tables.
//
//	<base table>.<column>.<foreign table>.<foreign column>
//
// and so on
type ColumnSelectorFull string

func (cs ColumnSelectorFull) String() string {
	return string(cs)
}

func (cs ColumnSelectorFull) StringLower() string {
	return strings.ToLower(string(cs))
}

// to string with quoted prefix and column
func (cs ColumnSelectorFull) StringQuoted() string {
	prefix, c := cs.SplitAtLastColumn()
	return fmt.Sprintf(`"%s"."%s"`, prefix, c)
}

func (cs ColumnSelectorFull) IsValid() bool {
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

func (cs ColumnSelectorFull) SplitAtLastColumn() (string, string) {
	idx := strings.LastIndex(string(cs), ".")
	return string(cs)[:idx], string(cs)[idx+1:]
}

func (cs ColumnSelectorFull) GetLastTable() Table {
	ts, _ := cs.Breakdown()
	return ts[len(ts)-1]
}

func (cs ColumnSelectorFull) ReplaceLastColumn(c Column) ColumnSelectorFull {
	idx := strings.LastIndex(string(cs), ".")
	return ColumnSelectorFull(string(cs)[:idx] + "." + c.String())
}

// get base table. Assumes that the column selector is valid
func (c ColumnSelectorFull) GetBasetable() Table {
	xs := strings.Split(string(c), ".")
	return Table(xs[0])
}

// breakdown column selector into table and column, where the same
// index are for the same table pair.
// Assumes that the column selector is valid
func (c ColumnSelectorFull) Breakdown() ([]Table, []Column) {
	xs := strings.Split(string(c), ".")
	tables := make([]Table, 0, len(xs)/2)
	columns := make([]Column, 0, len(xs)/2)
	for i := 0; i < len(xs); i += 2 {
		tables = append(tables, Table(xs[i]))
		columns = append(columns, Column(xs[i+1]))
	}
	return tables, columns
}

func ColumnSelectorRebuild(tables []Table, columns []Column) ColumnSelectorFull {
	if len(tables) != len(columns) {
		panic(fmt.Sprintf("invalid column selector: %v %v", tables, columns))
	}
	xs := make([]string, 0, len(tables)*2)
	for i := range tables {
		xs = append(xs, string(tables[i]), string(columns[i]))
	}
	return ColumnSelectorFull(strings.Join(xs, "."))
}

type ColumnMetadata struct {
	Name       Column          `json:"name"`
	DataType   DataType        `json:"dataType"`
	IsNullable bool            `json:"isNullable"`
	Relation   *ColumnRelation `json:"relation,omitempty"`
	Behavior   ColumnBehavior  `json:"behavior"`
}

func (c ColumnMetadata) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("missing column name")
	}
	if !c.Name.IsValid() {
		return fmt.Errorf("invalid column name")
	}
	if c.DataType == "" {
		return fmt.Errorf("missing column data type")
	}
	return nil
}

type ColumnRelation struct {
	Table  Table  `json:"table"`  // foreign table name
	Column Column `json:"column"` // foreign column name
}

type ColumnBehavior struct {
	Properties     map[string]string `json:"properties"`
	AllowSorting   bool              `json:"allowSorting"`
	AllowFiltering bool              `json:"allowFiltering"`
	// whether to disable, enable or use default option for filter operations
	OmitDefaultFilterOperations bool `json:"omitDefaultFilterOperations"`
	// set of allowed filter operations, in addition to the default ones
	FilterOperations []FilterOperator `json:"filterOperations"`
}
