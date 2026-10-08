# 219 — Metadata Follows a Move into Another DuckDB Database

## Evidence

On 185828f, a DuckDB console that ran `USE att`, `att` being an attached database, kept the cached tables of the database its URL opens: the server was in `att.main`, while completion and browsing offered the first database's tables until the next schema change, `clutch-switch-schema` or `clutch-refresh-schema`. Clearing the cache by hand loaded the tables of `att`, so only the decision to replace it was wrong; the guide stated it as a limit. After a statement that may move the console, Clutch compared the schema the server reports with the one the mode line showed before, and both databases' schema is `main`.

## Decision

- `clutch-db-metadata-scope` returns the scope of a connection's metadata when its current schema is not all of it, as the connection last reported it, without asking the server: nil by default, and on JDBC the catalog and schema its metadata requests name. The follow-up compares it as well as the schema, so a move into another catalog replaces the cached metadata, and a statement that moves nothing keeps it.
- It is read before the statement runs, with the namespace the mode line shows. When the follow-up runs, the connection has already recorded where it is: JDBC asks DuckDB for its catalog and schema as soon as such a statement returns, and PostgreSQL forgets its search_path then. Read in the follow-up, the scope would be compared with itself.
- The resolution context that results record is not compared: DuckDB gives it only by a query, which would run before each `USE`, `SET` or `RESET`, and on PostgreSQL it holds the whole search_path, so a change to a later schema of the path would reload the metadata of a current schema that did not change.
- `clutch-switch-schema` replaces the cached metadata itself, and on DuckDB it stays in the current catalog, so it is unchanged.

## Limits

- The mode line shows the schema only, `main` in either database.

## Verification

- A live DuckDB test fails on 185828f and passes here: after a `USE` of an attached database, the cached tables are that database's, and a `SET` that moves nothing keeps the same cached metadata.
- Unit tests cover the comparison when only the catalog moved, and the transaction commands passing what was read before the transaction ended.
- Each part, undone on its own, fails a test: comparing the schema alone, a JDBC scope of nil, reading the scope in the follow-up rather than before the statement, and replacing the metadata after every statement.
