# 209 — Metadata Follows the Qualified Table

## Evidence

A result of a table that the query qualified by its schema took its key and columns from the table of the same name in the connection's default namespace. On SQLite, `SELECT * FROM aux.people` used the key of `main.people`, so an edit built `UPDATE aux.people SET "name" = 'EDITED' WHERE "id" = 'same'`, which matched two rows and was rolled back by the one-row check. PostgreSQL 16 behaved the same for `other.people` beside `public.people`; with the same key column on both, `other.items` changed the right row but refused to edit a column that only it has; and a table outside the search path could not be edited at all, its metadata failing with `relation "only_here" does not exist`. Preparing a query already split a qualified source into schema and catalog, and row identity candidates and their cache took both, but only the JDBC adapter returned them, and column details, foreign keys and primary keys took a bare table name and were cached by it.

## Decision

- A table is its name with the schema and catalog that qualify it in the query, the shape row identity candidates and table comments already had. Column details, foreign keys and primary keys take SCHEMA and CATALOG as optional arguments, and `clutch-db--namespace-arguments` passes them only for a qualified table, so an unqualified one reaches every method as before. No structure joins the three, since it would be a third representation beside the source token and the identity plist.
- The table metadata cache keys a qualified table by `(CATALOG SCHEMA TABLE)`, as the row identity cache does, and an unqualified one by its name. `people` and `aux.people` may be different tables, so they are cached apart, and clearing a table by name drops both. The cache functions take this key where they took the table name, so their signatures stay.
- A result keeps the schema and catalog with its source table, through refreshing and filtering, and every metadata lookup for it, from cell edits and staged statements to the insert form and foreign keys, uses its key. SQL text still names the table as the query did.
- SQLite reads a qualified table in its attached database, prefixing PRAGMA and `sqlite_master` with the schema.
- PostgreSQL names a source table as it stores it: an unquoted part folds to lower case and a quoted one keeps its case, as JDBC upper-cases unquoted Oracle names. Each metadata query resolves the quoted name, qualified by its schema when the query qualified it, to an oid with `regclass`, so a qualified table is read in its schema and an unqualified one through the search path, as the query read it. Keys already resolved this way, while column details and foreign keys looked only in `current_schema()` and matched the name as written. Describing a table passes its schema, and the table comment uses it. XTDB, which has no foreign keys and resolves no `regclass`, returns none.
- A foreign key names the referenced table without its schema, which a qualified result resolved in the default namespace once it had its own foreign keys: following one of `aux.children` ran `SELECT * FROM "parents" WHERE "id" = 1` and opened `main.parents`. Foreign keys of a qualified table carry the referenced table's schema as `:ref-schema` where the backend knows it, and following one names that schema. SQLite keeps a foreign key within its database, and PostgreSQL's foreign-key query keeps the keys that reference a table in the same schema, so in both the schema is the table's own.

## Limits

- MySQL's `db.table` and JDBC column details still read the bare name.
- Foreign keys that reference a table in another schema, completion and Eldoc stay unqualified.

## Verification

- A real SQLite test attaches a database whose `people` has another key and an extra column, and checks that a result of `aux.people` takes its key, and that a staged UPDATE, DELETE and INSERT name `aux.people` with that key and leave `main.people` empty after submitting. It fails on 9c21ff3, where the key came from `main.people`.
- A real SQLite test follows a foreign key of `aux.children` beside a `main.parents` with the same key, and checks that it opens `aux.parents`. It fails on 3d534d1, which opened `main.parents`.
- Unit tests cover the key helpers, clearing a table by name, a metadata update that refreshes only the results of its qualified table, and PostgreSQL's folding.
- A PostgreSQL live test edits `other.people`, whose schema is off the search path while `public` has a table of the same name keyed by another column, through an upper-cased unquoted name, and a quoted `"Qual"."People"`; both change their own rows and `public` stays empty. It fails without the PostgreSQL change, where the key came from `public`.
- A PostgreSQL live test follows a foreign key of a table in a schema off the search path, beside a `public` parent table of the same name, and checks that it opens the parent in that schema. It fails without the PostgreSQL change, which opened the `public` one.
