package pgd

import (
	"fmt"

	"github.com/pkg/errors"
)

// data type. Lower case names of postgres data types
type DataType string

type Config struct {
	// database schema. Empty assumes <defaultSchema>
	Schema string `json:"schema"`

	DefaultLimit uint64 `json:"defaultLimit"`

	// define filter operations or use the DefaultFilterOperations
	FilterOperations FilterOperations

	// ColumnDefaults is a map of default column behaviors for specific data types
	ColumnDefaults map[DataType]ColumnBehavior `json:"columnDefaults"`
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
			if ops, exists := c.FilterOperations[dataType]; !exists {
				return fmt.Errorf("invalid config: filterOperation for data type '%s' is not present", dataType)
			} else if _, exists := ops[filter]; !exists {
				return fmt.Errorf("invalid config: filterOperation for combination of data type '%s' and filter '%s' is not registered", dataType, filter)
			}
		}

		if behavior.AllowFiltering && len(behavior.FilterOperations) == 0 && len(c.FilterOperations[dataType]) == 0 {
			return fmt.Errorf("invalid config: dataType '%s': allowFiltering is set, but filterOperations and default filter operations are both empty",
				dataType)
		}
	}

	return nil
}
