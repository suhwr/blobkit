package sql

import (
	"fmt"
	"strings"
)

// Dialect represents a supported SQL flavor.
type Dialect string

const (
	// DialectSQLite represents SQLite 3 syntax and semantics.
	DialectSQLite Dialect = "sqlite"

	// DialectPostgres represents PostgreSQL syntax and semantics.
	DialectPostgres Dialect = "postgres"
)

// Placeholder returns the query parameter placeholder for the given 1-based parameter index.
func (d Dialect) Placeholder(index int) string {
	switch d {
	case DialectPostgres:
		return fmt.Sprintf("$%d", index)
	default:
		return "?"
	}
}

// Placeholders generates a comma-separated list of placeholders starting at startIndex.
func (d Dialect) Placeholders(startIndex, count int) string {
	if count <= 0 {
		return ""
	}
	parts := make([]string, count)
	for i := 0; i < count; i++ {
		parts[i] = d.Placeholder(startIndex + i)
	}
	return strings.Join(parts, ", ")
}

// JSONType returns the database column type for JSON structures.
func (d Dialect) JSONType() string {
	switch d {
	case DialectPostgres:
		return "JSONB"
	default:
		return "TEXT"
	}
}

// TimestampType returns the database column type for timestamps.
func (d Dialect) TimestampType() string {
	switch d {
	case DialectPostgres:
		return "TIMESTAMP WITH TIME ZONE"
	default:
		return "DATETIME"
	}
}

// BooleanType returns the database column type for booleans.
func (d Dialect) BooleanType() string {
	switch d {
	case DialectPostgres:
		return "BOOLEAN"
	default:
		return "INTEGER"
	}
}
