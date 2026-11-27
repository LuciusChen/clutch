# Native Backends

Clutch's non-JDBC backends run without the Java sidecar:

- [`mysql`](https://github.com/LuciusChen/mysql.el) — pure Elisp MySQL wire-protocol client
- [`pgsql.el`](https://github.com/LuciusChen/pgsql.el) — pure Elisp PostgreSQL wire-protocol client
- `clutch-db-sqlite.el` — SQLite adapter using Emacs 29.1+'s built-in `sqlite-*` functions
- `clutch-mongodb.el` + `clutch-document.el` over `mongodb.el` — native MongoDB document backend
- `clutch-redis.el` over `redis.el` — native Redis key/value backend and command buffers

For JDBC backends, see [jdbc-backend.md](jdbc-backend.md) and [jdbc-agent-protocol.md](jdbc-agent-protocol.md). For support levels, see [backend-support.md](backend-support.md).

## MongoDB and Redis

See [mongodb-backend.md](mongodb-backend.md) for native MongoDB document consoles and `:surface sql-interface` (which uses JDBC). Redis connects via `redis.el` with line-oriented command buffers, bounded key browsing, and type-aware reads (see [backend-support.md](backend-support.md#redis)).

## SSH Tunnels and TRAMP

`:ssh-host` and `:tramp-default-directory` work identically across all native TCP backends; see [SSH tunnels](user-guide.md#ssh-tunnels-via-sshconfig) and [TRAMP-aware connection origin](user-guide.md#tramp-aware-connection-origin).

## MySQL (`:backend mysql`, `mysql.el`)

### Capabilities

- Pure Elisp (`HandshakeV10` / `HandshakeResponse41`, `COM_QUERY`, prepared statements)
- `mysql_native_password` and `caching_sha2_password` authentication
- TLS via Emacs GnuTLS

### Standalone Example

```elisp
(require 'mysql)

(let ((conn (mysql-connect :host "127.0.0.1"
                           :port 3306
                           :user "root"
                           :password "secret"
                           :database "mydb")))
  (unwind-protect
      (let ((result (mysql-query conn "SELECT * FROM users LIMIT 10")))
        (list (mysql-result-columns result)
              (mysql-result-rows result)))
    (mysql-disconnect conn)))
```

Prepared statements:

```elisp
(let ((stmt (mysql-prepare conn "SELECT * FROM users WHERE id = ?")))
  (unwind-protect
      (mysql-result-rows (mysql-execute stmt 42))
    (mysql-stmt-close stmt)))
```

### TLS

`:tls t` enables certificate and hostname verification by default (`mysql-tls-trustfiles`, `mysql-tls-verify-server`, `mysql-tls-keylist`). Pass `:ssl-mode disabled` (alias `off`) or `:tls nil` for plaintext and to skip automatic MySQL 8 TLS retries. For local MySQL 8 containers with self-signed certificates under `caching_sha2_password`, trust the CA or set `mysql-tls-verify-server` to `nil`.

### Transactions in Clutch

`C-c C-a` toggles `SET autocommit = 0/1`, `C-c C-m` commits, and `C-c C-u` rolls back, reflected in the header line (`Tx: Auto` / `Tx: Manual` / `Tx: Manual*`).

## PostgreSQL (`:backend pg`, `pgsql.el`)

### Capabilities

- Pure Elisp PostgreSQL v3 protocol (clear-text, MD5, SCRAM-SHA-256, GnuTLS)
- Simple and parameterized queries, OID-based decoding for scalars, dates/times, JSON, `bytea`, and arrays
- `ReadyForQuery` transaction state, structured server errors, and protocol-level `CancelRequest`
- Schema-qualified queries (such as `SELECT * FROM other.people`) resolve keys and columns from that schema even when it is off `search_path`, following PostgreSQL's quoting and case-folding rules

### Standalone Example

```elisp
(require 'pgsql)

(let ((conn (pgsql-connect :database "mydb"
                           :user "postgres"
                           :password "secret"
                           :host "127.0.0.1"
                           :port 5432
                           :sslmode 'prefer)))
  (unwind-protect
      (let ((result (pgsql-exec conn "SELECT * FROM users LIMIT 10")))
        (list (pgsql-result-columns result)
              (pgsql-result-rows result)))
    (pgsql-disconnect conn)))
```

Parameterized queries use `(value . type-name)` pairs (`pgsql-null` for SQL NULL, `nil` for boolean false); see the [`pgsql.el` README](https://github.com/LuciusChen/pgsql.el) for the full API:

```elisp
(pgsql-exec-params
 conn
 "SELECT $1::int4, $2::text, $3::boolean, $4::int4[]"
 (list (cons 42 "int4")
       (cons pgsql-null "text")
       (cons nil "bool")
       (cons (vector 1 pgsql-null 3) "int4[]")))
```

### TLS

`:sslmode` accepts `disable`, `prefer`, `require` (encrypted without verification), and `verify-full` (certificate + hostname verification). Clutch also maps `:tls t` / `:tls nil` to `require` / `disable`. Under `prefer`, plaintext fallback happens only if the server explicitly rejects SSL.

### Type Notes

- **Arrays with custom bounds** (e.g. `[0:2]={1,2,3}`): sent unchanged, but decoded to plain Lisp vectors without lower-bound metadata; inspect with `array_lower` if bounds matter.
- **`timestamptz`**: displayed in Emacs's local time without an offset. Values edited or sent without an offset receive Emacs's local offset for that timestamp (during a repeated DST hour, standard time is used) so the server stores the displayed time regardless of session time zone. Text inside `timestamptz[]` arrays is sent as written.

### Transactions in Clutch

PostgreSQL has no session autocommit toggle, so `C-c C-a` enables a Clutch-managed manual mode that sends `BEGIN` lazily on the first foreground statement (transactional DDL counts as uncommitted work). `C-c C-m` commits, `C-c C-u` rolls back, and transaction state comes directly from `ReadyForQuery`.

## XTDB (`:backend xtdb`, `pgsql.el`)

XTDB 2 speaks the PostgreSQL wire protocol, so Clutch connects via `pgsql.el` and reuses the PostgreSQL adapter with XTDB-specific adjustments:

```elisp
("xt" . (:backend xtdb :host "127.0.0.1" :port 5432
         :user "xtdb" :database "xtdb"))
```

- **Row identity & temporal queries:** Every table is keyed by `_id`. Queries of past versions (`FOR SYSTEM_TIME`, `FOR VALID_TIME`, `SETTING`) stay read-only.
- **Typed DML & union columns:** Clutch reads XTDB types from `information_schema.columns` (`:utf8`, `:i64`, etc.) and sends typed parameters in DML. Numeric union columns (e.g. `[:union :i64 :f64]`) stay editable by matching each value to a fitting integer, decimal, or float member; mixed non-numeric unions cannot map to one PostgreSQL parameter type and must be updated with SQL.
- **`timestamptz` & system columns:** Clutch attaches Emacs's local UTC offset (XTDB rejects offset-less `timestamptz` and keeps each value's offset as part of its column type). `_system_from`, `_system_to`, `_valid_from`, and `_valid_to` are treated as generated (though `INSERT` can set `_valid_from`/`_valid_to`), and every column except `_id` is optional on insert.
- **Transactions & `ERASE`:** XTDB has no savepoints, so staged edits submit in Auto mode only. Because XTDB cannot mix reads and writes in one transaction, Manual mode keeps plain reads outside a transaction and opens `BEGIN READ WRITE` on the first DML or `ASSERT` (use `BEGIN READ ONLY` explicitly for a read snapshot). `ERASE` asks for confirmation like `DELETE` and counts as uncommitted work in Manual mode. XTDB reports 0 affected rows for all DML, so Clutch cannot verify exact affected-row counts (`_id` is unique).
- **Schema & objects:** Only tables are listed (no comments, indexes, views, sequences, routines, or triggers). Schema stays `public`; `:schema` and schema switching are unavailable.

## SQLite (`:backend sqlite`)

Uses Emacs 29.1+'s built-in `sqlite-*` functions directly on a local file, with no network stack, TLS, sidecar, or timeouts (`:host`, `:port`, `:user`, `:ssh-host`, `:tramp-default-directory`, and schema switching do not apply).

```elisp
(setq clutch-connection-alist
      '(("dev-sqlite" . (:backend sqlite
                          :database "/tmp/demo.db"))))
```

Tables queried from attached databases (such as `SELECT * FROM aux.people`) take their keys and columns from that attached database.

## Shared Native-Backend Notes

### Timeouts and Interrupts

- MySQL accepts `:connect-timeout` and `:read-idle-timeout`. On synchronous read-idle timeout or `C-g`, Clutch sends `KILL QUERY` on a helper connection and drains the original socket (closing it if resync fails). With `mysql-query-async`, foreground statements do not block and have no idle timeout (metadata queries still do).
- PostgreSQL accepts `:connect-timeout`, `:read-idle-timeout`, and `:query-timeout` (mapped to `statement_timeout`). `C-g` calls `pgsql-cancel` (`CancelRequest` on an auxiliary socket) and drains through `ReadyForQuery` so the session stays usable.
- SQLite falls back to disconnect/reconnect on interrupt.

### Metadata, Completion, and DML

- MySQL and PostgreSQL defer initial schema snapshots and non-table object warmup until Emacs is idle, and keep completion/Eldoc cache-first so point motion is never blocked.
- Staged `INSERT`/`UPDATE`/`DELETE` run through backend parameter binding (while confirmation previews render readable SQL literals).
- Row identity falls back in order: primary key → non-null unique key → `ctid` (PostgreSQL) or `rowid` (SQLite). Physical locators can change after `UPDATE`, so add `ORDER BY` when row order matters.
- Object shortcuts (`C-c C-j`, `C-c C-d`, `C-c C-o`, `C-c C-l`) work identically across native and JDBC backends.
