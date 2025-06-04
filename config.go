package pgd

import (
	"fmt"
	"slices"

	"github.com/pkg/errors"
)

type DataType string

type Config struct {
	// database schema. Empty assumes <defaultSchema>
	Schema string `json:"schema"`

	DefaultLimit uint64 `json:"defaultLimit"`

	// define filter operations or use the DefaultFilterOperations
	FilterOperations FilterOperations

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
	if len(c.FilterOperations) == 0 {
		return errors.New("invalid config: filterOperations empty")
	}

	for dataType, behavior := range c.ColumnDefaults {
		for _, filter := range behavior.FilterOperations {
			if _, exists := c.FilterOperations[filter]; !exists {
				return errors.New("invalid config: filterOperation for column defaults is not registered in config FilterOperations")
			}
		}

		if behavior.AllowFiltering && len(behavior.FilterOperations) == 0 {
			return fmt.Errorf("invalid config: %s: filterOperations cannot be empty when allowFiltering is true", dataType)
		}
	}

	for _, filter := range c.ColumnUnknownDefault.FilterOperations {
		if _, exists := c.FilterOperations[filter]; !exists {
			return errors.New("invalid config: filterOperation for column unknown defaults is not registered in config FilterOperations")
		}
	}
	if c.ColumnUnknownDefault.AllowFiltering && len(c.ColumnUnknownDefault.FilterOperations) == 0 {
		return fmt.Errorf("invalid config: columnUnknownDefault: filterOperations cannot be empty when allowFiltering is true")
	}
	return nil
}

func (c Config) GetFilterOperations() []FilterOperator {
	keys := make([]FilterOperator, 0, len(DefaultFilterOperations))
	for k := range c.FilterOperations {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
