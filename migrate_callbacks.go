package dna

// migrationCallbacks stores custom Go functions to be called during migration
// for specific tables. Registered via WithMigration before calling New().
var migrationCallbacks = map[string]func(*Dna) error{}

// WithMigration registers a custom Go callback for a table's migration.
// The callback is called after schema migration (MigrateTable) succeeds
// but before the version record is updated.
// This is useful for complex data transformations that cannot be expressed
// as a simple migrate: tag expression.
func WithMigration(tableName string, fn func(*Dna) error) {
	migrationCallbacks[tableName] = fn
}
