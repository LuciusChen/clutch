# 212 — A Result Remembers Where Its Query Ran

## Evidence

A result's SQL names its tables as its query did, and the connection resolves those names wherever it is when that SQL runs. Each case was reproduced on c2bc7e1, and none gave a warning.

- MySQL: databases `a` and `b` each hold `t` with a row `id = 1`. A result of `SELECT id, v FROM t` shown in `a`, an edit of `v` staged, `clutch-switch-schema` to `b`, and submitting updated `b.t` and left `a.t` alone. With 2 rows in `a.t` and 3 in `b.t`, counting the result after the switch gave 3, and its second page showed `b.t`'s second row.
- PostgreSQL in Manual mode, where `current_schema()` stayed `s1` throughout: `SET search_path TO s1, s3` committed, then `SET search_path TO s1, s2` inside the open transaction, and a result of `t` showed `s2.t`'s row, with an edit staged. The session was lost; the next statement in the console reconnected on the recorded path `s1, s3` before it asked to discard the staged edit, which was kept; and submitting the edit wrote `s3.t`.
- A typed `USE` or `SET` does not get there only because a statement first asks to discard the staged edits of the connection's result buffer. On MySQL, whose result buffers are named by database, the `USE` then opens a new result buffer, and staging an edit in the old one and submitting it after the `USE` updated `b.t` too.
- DuckDB moved into an attached database with `USE att` keeps the schema name `main`, so the mode line shows `main` before and after. `UPDATE main.t` then updated `att.main.t`: a schema that qualifies a name is looked up in the current catalog. `current_catalog()`, `current_schema()` and `current_setting('search_path')` went from `home`, `main` and an empty path to `att`, `main` and `att.main`.

## Decision

- A backend method, `clutch-db-resolution-context`, returns what the server resolves an unqualified name in, compared only with `equal`. It defaults to the database and the current schema. PostgreSQL returns the database and `current_schemas(true)`, the effective path with the schemas it searches implicitly, cached in the connection and asked again after the SQL that `clutch-db-pg--namespace-statement-p` picks, so a later schema of the path that changes is seen although `current_schema()` stays. XTDB, which has no search path, keeps the default. DuckDB returns its catalog, schema and search path, asked each time, as its current schema is.
- A shown result records the context in the statement's completion, on the same session, once its rows have arrived; a context that cannot be read, or whose lookup is quit, is recorded as `unknown`, and the result is still shown, its activity ended. Refreshing it (`g`) and filtering it on the server run a new query, which records its own.
- Staging or submitting a change, loading a page, counting rows and exporting all rows first call `clutch-result--refuse-if-moved`, which refuses, with a message to switch back or run the query again, when the connection's context differs from the result's or the result's is `unknown`. A failure to read the connection's context, as in a PostgreSQL transaction that already failed, is reported as the server gave it. Nothing is checked while the connection is not live: staging only changes the buffer, and submitting does not reconnect, so it fails on its own.
- A table that the query qualifies by its schema is refused too, since DuckDB looks the schema up in the current catalog. On MySQL this refuses `a.t` after a switch to `b`, which would still reach `a.t`. Switching back lifts the refusal; running the query again does too, though on MySQL the new result opens in `b`'s result buffer, and the old buffer keeps refusing.
- Recording the shown namespace instead would miss PostgreSQL's later schemas and DuckDB's catalog. Qualifying the SQL with the recorded namespace would change the SQL the user sees, needs each backend's quoting, and cannot express a PostgreSQL path.

## Limits

- Moves that Clutch does not follow, which the guide lists, such as a PostgreSQL `SELECT set_config`, are not noticed. On PostgreSQL a schema on the path that is created or dropped, or the temporary schema that a first temporary table adds, is noticed only after the next statement that may change the path.
- Only where names resolve is compared, not which server or connection entry the result came from. No command moves a result's buffer to another server today: `clutch-connect` leaves the session's other buffers on the old connection, the automatic reconnect reuses their parameters, and a schema switch that reconnects stays on the same server.

## Verification

- A MySQL live test switches from `a` to `b` with an edit staged: submitting and counting are refused, `a.t` and `b.t` keep their values, and once the query runs again the edit updates `b.t`. It fails without the check, where submitting updated `b.t`.
- A PostgreSQL live test follows the Manual-mode path case above: submitting is refused and both `s2.t` and `s3.t` keep their values in the session. It fails without the check, where submitting wrote `s3.t`.
- A unit test fails the lookup with an error and with a quit after a successful reply: the result is shown with an `unknown` context, and no running time or reservation of the connection is left.
- Unit tests cover each refused command, an `unknown` context, a connection that is not live, the context recorded after the statement, PostgreSQL asking once and again after a `SET` and a `ROLLBACK`, and DuckDB's query.
