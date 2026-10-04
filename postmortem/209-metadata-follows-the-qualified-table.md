# 209 — Metadata Follows the Qualified Table

## Evidence

A result of a table that the query qualified by its schema took its key and columns from the table of the same name in the connection's default namespace. On SQLite, `SELECT * FROM aux.people` used the key of `main.people`, so an edit built `UPDATE aux.people SET "name" = 'EDITED' WHERE "id" = 'same'`, which matched two rows and was rolled back by the one-row check. PostgreSQL 16 behaved the same for `other.people` beside `public.people`; with the same key column on both, `other.items` changed the right row but refused to edit a column that only it has; and a table outside the search path could not be edited at all, its metadata failing with `relation "only_here" does not exist`. Preparing a query already split a qualified source into schema and catalog, and row identity candidates and their cache took both, but only the JDBC adapter returned them, and column details, foreign keys and primary keys took a bare table name and were cached by it.

## Decision

- A table is its name with the schema and catalog that qualify it in the query, the shape row identity candidates and table comments already had. Column details, foreign keys and primary keys take SCHEMA and CATALOG as optional arguments, and `clutch-db--namespace-arguments` passes them only for a qualified table, so an unqualified one reaches every method as before. No structure joins the three, since it would be a third representation beside the source token and the identity plist.
- The table metadata cache keys a qualified table by `(CATALOG SCHEMA TABLE)`, as the row identity cache does, and an unqualified one by its name. `people` and `aux.people` may be different tables, so they are cached apart, and clearing a table by name drops both. The cache functions take this key where they took the table name, so their signatures stay.
- A result keeps the schema and catalog with its source table, through refreshing and filtering, and every metadata lookup for it, from cell edits and staged statements to the insert form and foreign keys, uses its key. SQL text still names the table as the query did.
- SQLite reads a qualified table in its attached database, prefixing PRAGMA and `sqlite_master` with the schema.
- A foreign key names the referenced table without its schema, which a qualified result resolved in the default namespace once it had its own foreign keys: following one of `aux.children` ran `SELECT * FROM "parents" WHERE "id" = 1` and opened `main.parents`. Foreign keys of a qualified table carry the referenced table's schema as `:ref-schema` where the backend knows it, and following one names that schema. SQLite keeps a foreign key within its database, so the schema is the table's own.

## Limits

- PostgreSQL, MySQL's `db.table` and JDBC column details still read the bare name. PostgreSQL also folds unquoted names to lower case, which the qualified lookup has to follow, so it comes in its own change.
- Foreign keys that reference a table in another schema, completion and Eldoc stay unqualified.

## Verification

- A real SQLite test attaches a database whose `people` has another key and an extra column, and checks that a result of `aux.people` takes its key, and that a staged UPDATE, DELETE and INSERT name `aux.people` with that key and leave `main.people` empty after submitting. It fails on 9c21ff3, where the key came from `main.people`.
- A real SQLite test follows a foreign key of `aux.children` beside a `main.parents` with the same key, and checks that it opens `aux.parents`. It fails on 3d534d1, which opened `main.parents`.
- Unit tests cover the key helpers, clearing a table by name, and a metadata update that refreshes only the results of its qualified table.
