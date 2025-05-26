package pgd

import (
	"fmt"
	"slices"
)

type DataType string

type Config struct {
	// database schema. Empty assumes <defaultSchema>
	Schema string `json:"schema"`

	DefaultLimit uint64 `json:"defaultLimit"`

	// ColumnDefaults is a map of default column behaviors for specific data types
	ColumnDefaults map[DataType]ColumnBehavior `json:"columnDefaults"`

	// Column Behavior for unknown data types
	ColumnUnknownDefault ColumnBehavior `json:"columnUnknownDefault"`
}

func (c *Config) Validate() error {
	if c.Schema == "" {
		return fmt.Errorf("invalid config: schema cannot be empty")
	}
	if c.DefaultLimit == 0 {
		return fmt.Errorf("invalid config: defaultLimit not set")
	}
	if c.DefaultLimit > maxLimit {
		return fmt.Errorf("invalid config: defaultLimit above maxLimit")
	}
	for dataType, behavior := range c.ColumnDefaults {
		if slices.Contains(behavior.FilterOperations, "") {
			return fmt.Errorf("invalid config: %s: filterOperations cannot contain empty strings", dataType)
		}

		if behavior.AllowFiltering && len(behavior.FilterOperations) == 0 {
			return fmt.Errorf("invalid config: %s: filterOperations cannot be empty when allowFiltering is true", dataType)
		}
	}

	if slices.Contains(c.ColumnUnknownDefault.FilterOperations, "") {
		return fmt.Errorf("invalid config: columnUnknownDefault: filterOperations cannot contain empty strings")
	}
	if c.ColumnUnknownDefault.AllowFiltering && len(c.ColumnUnknownDefault.FilterOperations) == 0 {
		return fmt.Errorf("invalid config: columnUnknownDefault: filterOperations cannot be empty when allowFiltering is true")
	}
	return nil
}
