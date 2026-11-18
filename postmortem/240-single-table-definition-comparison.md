# Compare table definitions in Ediff

The object picker needs entries qualified by schema to distinguish same-name tables. The comparison asks for a fresh catalogue on a short-lived target connection and passes the selected entry to the existing DDL reader. The target picker uses the usual annotations and defaults. Clutch's Embark actions resolve entries on the invoking connection, so the target picker exposes selection only. Oracle lists visible tables and views with one catalogue query that includes each object's owner. Ordinary table discovery retains the current schema scope.

Ediff compares snapshots of the existing DDL display text. Each side identifies the connection and table in the buffer name and header, leaving the definition itself untouched. Ediff cleanup discards both snapshots and restores the invoking window configuration. The displayed coverage is the coverage of the original DDL reader, including its reconstructed definitions.
