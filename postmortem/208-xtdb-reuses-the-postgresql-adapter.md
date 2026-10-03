# 208 — XTDB Reuses the PostgreSQL Adapter

## Evidence

A user asked for XTDB 2. XTDB speaks the PostgreSQL wire protocol, and against XTDB 2.1.0 Clutch's PostgreSQL backend connected, ran queries and listed tables through `pg_tables`, but no result could be edited and several catalog queries failed. The primary-key query uses `indkey::smallint[]`, `generate_subscripts` and `::regclass`, which XTDB cannot parse, so results had no row identity; XTDB keys every table by `_id`, and its `information_schema` holds no constraints. Column details call `col_description`, table entries `obj_description`, and sequences read `pg_sequences`, none of which XTDB has. XTDB accepts no `CREATE` statement but `CREATE USER`, so it has no indexes, sequences, views, routines or triggers. It refuses DML parameters without a type and stores a parameter with the type it is sent as: an integer sent as text turns the column into `[:union :i64 :utf8]` without an error. `information_schema.columns` gives XTDB's own types, such as `:utf8`, `[:? :i64]` or `[:timestamp-tz :micro "+08:00"]`. A result column of a type that PostgreSQL has none for, such as a `time` or a union, is reported as `json`, and a `json` parameter is stored as the value it decodes to: a JSON string sent for a `time` column turned the column into `[:union [:time-local :nano] :utf8]`. Every `INSERT`, `UPDATE` and `DELETE` reports zero rows, which fails the exactly-one-row check on a staged `UPDATE` or `DELETE`, and there is no `SAVEPOINT`, which Manual mode uses for staged changes. XTDB refuses `timestamptz` text without a UTC offset, and it keeps a value's offset as part of its type. `SET search_path` takes only a string literal, and `current_schema()` stays `public` whatever it is set to. XTDB refuses an update of `_system_from`, `_system_to`, `_valid_from` or `_valid_to` and an insert of the `_system_` columns, while an insert may set `_valid_from` and `_valid_to`.

## Decision

- The xtdb backend is the PostgreSQL adapter with a connection subtype, in a `;;;; XTDB` section of `clutch-db-pg.el`. A file belongs to a native protocol implementation, and XTDB has no protocol of its own. Its registry entry names its own connect function, as each JDBC driver's does, and methods on the subtype override the adapter only where XTDB differs.
- Two alternatives were rejected. A folder for products that speak another database's protocol would break that rule and share the adapter's private structure across files. Capabilities declared in the backend registry for the adapter to read, such as a catalog flavor or savepoint support, would be a second dispatch mechanism beside `cl-defmethod`, while every other registry key is read by generic code; and an `information_schema` catalog flavor would be XTDB under another name, since PostgreSQL-protocol products such as CockroachDB have `pg_catalog`. As postmortem 113 says of key/value backends, a shared layer waits for a second concrete backend.
- `_id` is the primary key. Column details read `information_schema.columns` and map XTDB's types to the PostgreSQL type shown and sent for a staged value; a type with no single PostgreSQL type keeps XTDB's name and no parameter type, so XTDB refuses the value rather than storing it in another type. The four system columns are generated, which stops an update before it is staged and leaves `_valid_from` and `_valid_to` in the insert form, which sets valid time with them.
- A result column that XTDB reports as `json` carries no type. The adapter converts result columns through one method, which the subtype overrides, so a staged value for such a column takes its type from the column details: a `time` is sent as a `time`, and a union or a list, which maps to no single type, has none and is refused, where a `json` parameter would have changed the column's type. The cost is the JSON viewer and JSON validation for list and struct columns, whose values still show as JSON.
- Parameterized DML leaves its affected-row count unknown, so the row-count check is skipped. `_id` is unique, so a staged statement matches at most one row; what is lost is noticing a row that has gone.
- Manual mode refuses staged changes before any statement runs. Auto mode wraps them in `BEGIN` and `COMMIT`, which XTDB has.
- Object categories and the schema list are empty, so the object browser lists tables and schema switching is unavailable, as on backends without schemas. A connection that sets `:schema` is refused before it connects, instead of failing on the `SET search_path` that would apply it.

## Limits

- A `timestamptz` carries the offset of Emacs's time zone, as postmortem 207 has it for PostgreSQL, so a column whose values have another offset then holds both, as in `[:union [:timestamp-tz :micro "Z"] [:timestamp-tz :micro "+08:00"]]`. The times are unchanged, and the column still reads and edits as `timestamptz`.
- Columns of several types, and lists and structs, are changed with SQL, since XTDB needs a type for every parameter and they have none.
- A table's definition is a `CREATE TABLE` assembled from `information_schema` with XTDB's type names, although XTDB has no `CREATE TABLE`.
- The native live suite has no XTDB container yet.

## Verification

Checked by hand against XTDB 2.1.0:

- Connecting with the PostgreSQL keys; a connection with `:schema` refused before connecting.
- Tables listed; schema switching reported as unavailable; every object category empty.
- `_id` as the row identity; a row inserted from the insert form, a cell edit and a deletion, submitted in Auto mode, with the column types unchanged afterwards.
- `date`, `timestamp` and `time` columns inserted and edited, a time given as `HH:MM:SS` and as `HH:MM`; a JSON string for a time refused by XTDB.
- `timestamptz` cells edited and inserted in a column of `+08:00` values and in one of `Z` values, each stored as the time shown; `_valid_from` set from the insert form.
- A value for a union column refused, leaving the row and the column type unchanged.
- A submission in Manual mode refused, leaving the row unchanged.
