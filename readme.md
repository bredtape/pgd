# Data Discovery and Query Module

## Overview

This Golang module provides functionality for discovering data structures/schemas and performing queries against a Postgres database.

## Column metadata

Comments may be placed on columns to provide additional metadata. The comment must be in JSON format and contain the following fields:

```json
{
  "properties": { "key1": "value1", "key2": "value2" },
  "allowSorting": "bool",
  "allowFiltering": "bool",
  "omitDefaultFilterOperations": "bool",
  "filterOperations": ["string"]
}
```

All fields are optional and if not set, will use the default values provided in the `Config` struct.

## Issues

- Sorting on nullable columns ascending should have non-null values first and
  sorting descending should have null values first.

## Wish list

- Implement helper/method to expand wildcards in Query.Select. A \* replaces substitutes for any table/column (but does not match .) and a > includes everything remaining.
