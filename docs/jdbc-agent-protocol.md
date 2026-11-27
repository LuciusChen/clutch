# JDBC Agent Protocol

Wire protocol and session model between `clutch-db-jdbc.el` and [`clutch-jdbc-agent`](https://github.com/LuciusChen/clutch-jdbc-agent). For installation and user configuration, see [jdbc-backend.md](jdbc-backend.md).

## Runtime model

Clutch runs `clutch-jdbc-agent` as a local JVM sidecar (Java 17+) and exchanges one JSON object per line over stdin/stdout. Before loading third-party drivers, the agent saves the raw stdout handle for JSON replies and redirects Java's `System.out` to UTF-8 stderr so driver console output never corrupts framing while still appearing in Clutch diagnostics.

Each logical JDBC connection owns two underlying JDBC sessions, plus a third on Oracle:

- `primary`: foreground SQL, DML, DDL, and transactions
- `metadata`: schema and object introspection
- `bulk` (Oracle only, opened lazily): schema-wide listings (`get-tables`, `get-sequences`, `get-procedures`, `get-functions`, and table-less `get-indexes` / `get-triggers`)

Keeping sessions separate lets foreground SQL and metadata run independently, and on Oracle prevents long schema-wide listings from blocking interactive lookups like primary-key discovery (if Oracle rejects the extra logon, e.g. `ORA-02391`, schema-wide listings fall back to `metadata`).

- **Session recovery:** If a `metadata` or `bulk` session fails (e.g. `ORA-12592`), a lookup retries once on a replacement session with the remembered schema, leaving `primary` and its transaction untouched. Open cursors stay pinned to the session that opened them; if it fails mid-fetch, the cursor is invalidated and the listing must restart.
- **Namespace updates:** `set-current-schema` (Oracle) updates `primary` and `metadata` together and drops `bulk` so the next listing reopens in the new schema. DuckDB and SQL Server scope metadata to the namespace reported by `primary` after `USE`; SQL Server can advertise its current `catalog` in connect and foreground replies (older agents fall back to `DB_NAME()`). ClickHouse reconnects on database switch.
- **Locking & bypass:** Requests enter a thread pool and serialize per session under separate `primary`, `metadata`, and `bulk` locks (acquired in that order for schema changes and normal disconnect). `cancel` and `force-disconnect` bypass these locks to interrupt queued/running statements or drop a hung session immediately.

## Message shape

Requests:

```json
{"id":1,"op":"connect","params":{"url":"jdbc:oracle:thin:@//db:1521/FREEPDB1","driver-class":"oracle.jdbc.OracleDriver","user":"system","password":"secret"}}
```

Success response:

```json
{"id":1,"ok":true,"result":{"conn-id":7}}
```

Error response (optional `diag`, plus `debug` only when `params.debug=true`):

```json
{"id":1,"ok":false,"error":"connect failed","diag":{"category":"connect","op":"connect","request-id":1,"context":{"redacted-url":"jdbc:oracle:thin:@//db:1521/FREEPDB1"}}}
```

```json
{"id":1,"ok":false,"error":"connect failed","diag":{"category":"connect","op":"connect","request-id":1},"debug":{"thread":"clutch-jdbc-request","request-context":{"redacted-url":"jdbc:oracle:thin:@//db:1521/FREEPDB1?password=<redacted>","user":"system","property-keys":["oracle.net.wallet_location"]},"stack-trace":"java.sql.SQLException: connect failed\n..."}}
```

- `id` is client-generated and echoed back verbatim; `op` names the RPC; `params` holds operation arguments (`connect` requires `driver-class`).
- `ok=true` carries `result`. `ok=false` carries a short user-facing `error`, optional structured `diag` (including `diag.context.generated-sql` when internal SQL fails), and optional redacted `debug` only when requested via `params.debug=true`.

## Core operations

- **Connection & transactions:** `connect`, `disconnect`, `force-disconnect`, `ping`, `commit`, `rollback`, `set-auto-commit`, `create-savepoint`, `rollback-savepoint`, `release-savepoint`, `set-current-schema`
- **Execution & cursors:** `cancel`, `execute`, `execute-params`, `fetch`, `close-cursor`
- **Metadata:** `get-schemas`, `get-tables`, `search-tables`, `get-columns`, `search-columns`, `describe-source-columns` (SQL Server, advertised at connect), `get-primary-keys`, `get-foreign-keys`, `get-referencing-keys` (agent 0.2.28+), `get-indexes`, `get-index-columns`, `get-sequences`, `get-procedures`, `get-functions`, `get-procedure-params`, `get-function-params`, `get-triggers`, `get-object-source`, `get-object-ddl`, `get-referencing-objects`

## Connection and execution semantics

### Connect & idle validation

`connect` accepts `url`, `driver-class`, `user`, `password`, `props`, `connect-timeout-seconds`, `network-timeout-seconds` (metadata/bulk sessions only; primary has no socket timeout), `auto-commit`, and `validate-after-idle-seconds`. It returns `conn-id`, scoped to the current agent process (Clutch validates the owning process before sending requests so restarted agents never collide on reused IDs).

When `validate-after-idle-seconds` is positive, an idle primary session runs `Connection.isValid(3)` right before `execute`/`execute-params` creates a statement (metadata traffic does not reset primary idle time); idle metadata/bulk sessions run the same check before metadata requests and are replaced automatically if dead.

### Savepoints

`create-savepoint` (rejecting auto-commit sessions and checking `supportsSavepoints()`), `rollback-savepoint`, and `release-savepoint` expose opaque `savepoint-id` handles so SQL savepoint syntax stays out of Elisp. Commit, rollback, auto-commit changes, and disconnect invalidate handles.

### Cancel

`cancel` takes `conn-id` and optional `request-id` (agent 0.2.31+ can cancel queued statements before execution starts; omitting it targets only a running statement). It returns `conn-id`, `request-id` (if accepted), and `cancelled` (`true`/`false`). A statement stopped at entry returns diagnostic category `cancel`; for a running statement, the original reply reports the real outcome.

### Execute, binary parameters, and cursors

`execute` takes `conn-id`, `sql`, `fetch-size` (1–10,000, default 500), and `query-timeout-seconds`; `execute-params` adds `values`. `fetch` takes `cursor-id`, `fetch-size`, and `query-timeout-seconds`; `close-cursor` takes `cursor-id`.

- **Query timeout:** Positive `query-timeout-seconds` calls `Statement.setQueryTimeout` and gives the agent +1s grace before cancelling; `0` means no limit (agents before 0.2.26 treat `0` like omission, which defaults to 29s); negative values are rejected. Async foreground queries pass the configured limit (or `0`); blocking requests always carry a positive timeout capped at `rpc-timeout - 5s` (min 1s).
- **Warnings:** `execute` / `execute-params` replies include `messages` (up to 100 `SQLWarning` entries oldest-first, plus a summary count if more exist).
- **Binary parameter envelope:** Binary values in `execute-params` use `{"__clutch_jdbc_param":"binary","jdbc-type":"BLOB","base64":"..."}`. `base64: null` binds a typed SQL `NULL`; `""` binds a zero-byte `Blob`; non-empty BLOBs bind via `setBinaryStream` with exact length; RAW/BINARY types bind via `setBytes`. Malformed envelopes fail before execution.
- **BLOB/CLOB results:** Agent 0.2.30+ includes up to 256 leading bytes in `preview-base64` on `__type: "blob"` values (`length` always gives total bytes; JDBC `Blob` locators >64 KiB omit bytes to avoid extra reads; `ByteBuffer` reads from its current position without advancing it). Previews without complete decoded `text` stay read-only in Clutch, as do CLOBs whose 256 UTF-16-unit preview is shorter than their reported length.

## Metadata payloads

- **`get-tables`:** A cursor (`columns`, `rows`, `cursor-id`, `done`) with `name`, `type`, `schema`, and `source_schema`. Oracle returns these four columns. Other JDBC backends also return `comment`, with `null` for missing or blank JDBC `REMARKS`.
- **`search-tables`:** Returns a `tables` array of entries with `source-schema`. Oracle row identity supplies `table` for an exact base-table lookup in `schema`, alongside `prefix` for older agents. Completion supplies only the prefix and includes views and synonyms.
- **`get-columns` / `search-columns`:** Objects with `name`, `type`, `nullable`, `position`, and optional `default` (`COLUMN_DEF` / Oracle `DATA_DEFAULT`, omitted when unreported). On DuckDB and SQL Server, `get-columns` uses JDBC `LIKE` patterns, so Clutch escapes `_`, `%`, and `\`.
- **`describe-source-columns` (SQL Server):** Accepts `conn-id` and a `projection` SELECT (optionally preceded by `USE`), inspects the projection's source columns on the metadata session without executing, and returns `columns` in projection order (`null` when unresolvable).
- **`get-foreign-keys` / `get-referencing-keys` (agent 0.2.28+):** `get-foreign-keys` returns `fk-column`, `pk-table`, `pk-schema`, `pk-column`, and optional `fk-name`, grouped by target and ordered by `KEY_SEQ`. `get-referencing-keys` returns `fk-table`, `fk-schema`, `fk-column`, `pk-column`, and optional `fk-name` in key order. On JDBC, ER cancellation stops sending further RPCs after the active reply without sending `cancel` (protecting primary SQL).

## Error semantics

Five failure cases are handled differently:

1. **Normal database error** (syntax error, missing object, cancelled statement): agent stays up; Elisp shows `error` and records optional `diag`.
2. **Idle preflight failed before statement creation:** returns `diag.connection-invalidated=true` and `diag.execution-not-started=true`. Clutch may reconnect and run the statement once if transaction/batch state allows.
3. **Foreground failure after preflight invalidates the connection:** returns `diag.connection-invalidated=true` only (or top-level `connection-invalidated=true` if the statement succeeded before the connection closed). Elisp retires that `conn-id` without replaying SQL; the next command reconnects.
4. **Outer RPC timeout:** for a connection-owned synchronous request, Elisp retires that connection and sends `force-disconnect` without blocking (other connections keep the shared agent). Ownerless timeouts (startup/connect) or a dead agent stop the agent and invalidate all its connections. Async metadata timeouts expire their callback without dropping the connection.
5. **Agent exits before replying:** Elisp reads stderr directly (surfacing `UnsupportedClassVersionError` as a Java 17+ version mismatch rather than a generic disconnect).

`diag.connection-invalidated` and `diag.execution-not-started` come directly from the agent's connection map and execution lifecycle; clients never infer them from exception classes, SQLState, or error text.
