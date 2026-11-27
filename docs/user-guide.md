# User Guide

This guide covers configuring connections, running queries, handling transactions, storing passwords, tunneling over SSH or TRAMP, and working with `.sql` files. For full key bindings, result-grid commands, ER views, and customization options, see the [interactive client guide](interactive-client.md).

## Saved Connections and Query Workflow

### 1. Configure connections

```emacs-lisp
(require 'clutch)

(setq clutch-connect-timeout-seconds 10
      clutch-read-idle-timeout-seconds 30
      clutch-query-timeout-seconds 20
      clutch-jdbc-rpc-timeout-seconds 15
      clutch-jdbc-validate-after-idle-seconds 300)

(setq clutch-connection-alist
      '(("dev-mysql"  . (:backend mysql
                          :host "127.0.0.1" :port 3306
                          :user "root"
                          :database "mydb"
                          :connect-timeout 5
                          :read-idle-timeout 60))
        ("dev-pg"     . (:backend pg
                          :host "127.0.0.1" :port 5432
                          :user "postgres"
                          :database "mydb"))
        ("prod-pg-ssh" . (:backend pg
                          :host "pg.internal" :port 5432
                          :user "app" :database "appdb"
                          :ssh-host "bastion-prod"))
        ("remote-pg"  . (:backend pg
                          :host "127.0.0.1" :port 55433
                          :user "app" :database "appdb"
                          :tramp-default-directory "/ssh:devbox:/workspace/"))
        ("dev-redis"  . (:backend redis
                          :host "127.0.0.1" :port 6379
                          :database 0))
        ("dev-sqlite" . (:backend sqlite
                          :database "/path/to/my.db"))))
```

Key connection fields:

- `:backend`: `mysql`, `pg`, `xtdb`, `sqlite`, `mongodb`, `redis`, or a JDBC backend symbol such as `oracle`, `sqlserver`, `clickhouse`, `snowflake`, `redshift`, `db2`, or `cassandra` (use `:backend jdbc` only when no dedicated backend exists; see [JDBC backend guide](jdbc-backend.md)). Keeping `:backend` in the alist lets completion show backend icons without decrypting `:profile-entry`.
- `:database`: For SQLite, this is the only required field (use `":memory:"` for an in-memory database; do not combine SQLite with `:ssh-host` or `:tramp-default-directory`).
- `:password` / `:pass-entry` / `:profile-entry`: Optional credentials and encrypted profiles; see [Password Management](#password-management).
- Timeouts: SQL network backends (MySQL, PostgreSQL, XTDB, and JDBC) accept `:connect-timeout` and `:read-idle-timeout`; PostgreSQL and JDBC also accept `:query-timeout`; JDBC adds `:rpc-timeout` (native MongoDB and Redis use `mongodb-*` and `redis-*` timeout variables instead). Globally, `clutch-query-timeout-seconds` defaults to `nil` (no limit), `clutch-jdbc-rpc-timeout-seconds` to 30, and `clutch-jdbc-validate-after-idle-seconds` to 300 (set to `nil` or 0 to disable pre-execution JDBC idle checks).
- TLS: Pass `:tls t` to require TLS, or `:ssl-mode disabled` (MySQL) / `:sslmode disable` (PostgreSQL) for plaintext. See [Custom TLS certificates](#custom-tls-certificates).
- `:ssh-host` / `:tramp-default-directory`: Forward structured `:host` / `:port` endpoints through OpenSSH or TRAMP.
- `:relations-file`: Optional path (`~` or absolute) for storing manual ER relations (useful for sharing in Git). By default each connection gets its own file in `clutch-er-relations-directory` keyed by connection identity, so renaming a saved connection keeps both its relations and its saved console SQL.

### 2. Open a query console

```text
M-x clutch-query-console     ;; Pick an open console or saved connection, or press RET on a new name for a temporary connection
M-x clutch-query-sqlite-file ;; Shortcut to open a local SQLite file in a console
```

`clutch-query-console` shows both open consoles and saved connections; picking an existing console switches to it instead of opening a duplicate. If no saved connections exist, or you press `RET` on an unmatched name, it walks you through creating a temporary connection (including SQLite).

### 3. Write and run SQL

```sql
SELECT * FROM users LIMIT 10;
```

Press `C-c C-c` to run the selected region, or the statement at point if nothing is selected. Results appear in a split buffer below.

- **Exporting directly to a file:** Run `M-x clutch-export-query` (or `C-c ? e`) on a read-only `SELECT` to write CSV or TSV directly without opening a result grid. Defaults are controlled by `clutch-export-default-format`, `clutch-csv-export-default-coding-system`, `clutch-export-default-directory`, `clutch-export-default-file-name`, `clutch-export-null-value-text`, and `clutch-csv-delimiter` (details in [Export a query to a file](interactive-client.md#export-a-query-to-a-file)).
- **Large scripts and line numbers:** Long scripts and large exports refresh the SQL buffer frequently for progress updates, and `display-line-numbers-mode` adds noticeable overhead (roughly doubling a 3,200-statement `UPDATE` batch from ~14s to ~28s). Turn it off in SQL buffers if you run large batches: `(add-hook 'clutch-mode-hook (lambda () (display-line-numbers-mode -1)))`.
- **High-risk SQL guard:** By default, `TRUNCATE` and `UPDATE`/`DELETE` without an effective `WHERE` (including inside PostgreSQL `WITH` clauses or DB2 data-change tables) require typing `YES`. Customize `clutch-high-risk-query-confirmation` to switch to a normal `yes-or-no` prompt or disable it.

### 4. Control transactions

On native MySQL, native PostgreSQL, and JDBC connections that support manual commit, the same shortcuts work in the console, Result Browser, and Record view:

- `C-c C-a`: toggle auto-commit
- `C-c C-m` (`clutch-commit`): commit
- `C-c C-u` (`clutch-rollback`): roll back

Staged grid edits follow the active transaction mode:

- **Auto mode:** `C-c C-c` (`clutch-result-submit`) in a result buffer submits and commits the staged `INSERT`/`UPDATE`/`DELETE` batch automatically.
- **Manual mode:** `C-c C-c` wraps the batch in a savepoint inside the current transaction. If a later statement fails, Clutch rolls back only that batch so your earlier work stays intact and you can fix and retry; `C-c C-m` commits the outer transaction and `C-c C-u` rolls it back. Drivers without savepoint support (such as DuckDB JDBC) reject Manual-mode staged submission; switch to Auto mode first.

Backend notes:

- **SQLite** has no interactive transaction toggle (the header omits `Tx`).
- **Native MySQL** toggles the server session's autocommit directly. **Native PostgreSQL** uses a client-managed manual mode that sends `BEGIN` lazily on the first foreground statement (transactional DDL also counts as uncommitted work).
- **Server-reported open transactions:** On native MySQL, PostgreSQL, and XTDB, Clutch follows the server's transaction flag even in Auto mode. Typing `BEGIN` followed by a write shows `Tx: Auto*` and warns before closing; end it with `COMMIT`/`ROLLBACK` or press `C-c C-a` to switch to Manual mode while keeping the open transaction.
- **Uncertain state (`Tx: Uncertain`):** If batch recovery fails or a `COMMIT` finishes with an unknown outcome, Clutch blocks further queries, commits, and mode toggles until you run `C-c C-u` (rollback) or reconnect. Always verify the database state before retrying after an uncertain commit.
- **Lost vs disconnected sessions:** After an unexpected drop, cell editing, `UPDATE` copy, and staged submit reconnect automatically in the same commit mode (refusing if the reconnect lands on a different database/schema than the result). After an explicit disconnect, loaded rows stay readable, while actions requiring a live connection report that it is closed.

## Row-editing boundaries

Clutch enables staged `UPDATE` and `DELETE` only when it can uniquely identify a source row:

| Backend | Fallback when a table has no primary key |
|---|---|
| MySQL native | Non-null unique keys (no physical row locator) |
| PostgreSQL native | Non-null unique keys, then `ctid` on ordinary heap tables |
| XTDB native | Every table is keyed by `_id` |
| SQLite native | Non-null unique keys, then the first unshadowed rowid alias (`rowid`, `_rowid_`, `oid`) on rowid tables |
| Oracle JDBC | Non-null unique keys, then `ROWID` on confirmed base tables |
| Other JDBC backends | Non-null unique keys only |

Queries that join, group, aggregate, or combine rows stay read-only, as do temporal queries (`FOR SYSTEM_TIME`, Oracle `AS OF TIMESTAMP`) and Oracle views/synonyms (such as `USER_TABLES`).

Simple CTE queries over a single table are editable (Clutch threads row identity through the CTEs and maps column aliases back to the base table), except when a CTE joins, aggregates, is recursive, is referenced more than once, or shares a name with one of its columns (renaming the CTE fixes that). A CTE column list over `SELECT *` (such as `WITH c (k, v) AS (SELECT * FROM t)`) allows deletes but not column edits, since only the table knows the column order of `*`.

## Custom TLS certificates

To trust a custom CA bundle in Emacs GnuTLS:

```elisp
(with-eval-after-load 'gnutls
  (add-to-list 'gnutls-trustfiles
               (expand-file-name "certs/database-ca.pem" user-emacs-directory)))
```

Make sure the connection hostname matches the certificate, then use `:sslmode verify-full` on PostgreSQL or `:tls t` on MySQL and native MongoDB. If you set `mysql-tls-trustfiles` to a non-empty list, add the CA there too (it overrides `gnutls-trustfiles` for MySQL).

Note that Clutch's SSH and TRAMP tunnels connect to `127.0.0.1` locally, so TLS hostname verification against a certificate issued only for the remote hostname will fail. For JDBC TLS settings, use driver `:props` or JVM truststore flags (see [JDBC connection properties](jdbc-backend.md#additional-jdbc-connection-properties)).

## Password Management

You can put `:password` directly in a connection plist, though `auth-source` is safer. When `:password` is omitted, Clutch checks in order:

1. An explicit `:profile-entry` containing a password
2. `pass` via `auth-source-pass`, matching an explicit `:pass-entry` or the connection name
3. Standard `auth-source-search` by `:host`, `:user`, and `:port`
4. An interactive `read-passwd` prompt

```emacs-lisp
(require 'auth-source-pass)

(setq clutch-connection-alist
      '(("dev-mysql" . (:backend mysql
                         :host "db.example.com" :port 3306
                         :user "alice" :database "mydb"))))
```

```bash
pass insert mysql/dev-mysql   # or: pass insert dev-mysql
```

On JDBC connections (including Org-Babel blocks), an explicit `:pass-entry` that returns no password fails immediately in Emacs instead of passing a null password to the driver.

### Encrypted connection profiles

Use `:profile-entry` if you want to keep hosts, users, and database names out of your Emacs config as well:

```emacs-lisp
(setq clutch-connection-alist
      '(("prod-mysql" . (:backend mysql :profile-entry "mysql/prod"))
        ("prod-reporting" . (:backend mysql :profile-entry "mysql/reporting"))))
```

Explicit keys in `clutch-connection-alist` override fields from the profile, which makes it easy to define variants (such as a read-only database or an SSH route) over one encrypted entry:

```emacs-lisp
("prod-mysql-ro" . (:backend mysql :profile-entry "mysql/prod"
                    :database "app_readonly"))
```

In `pass` (`pass edit mysql/prod`), put the password on line 1 and fields below as `key: value`:

```text
db-password-here
backend: mysql
host: db.example.com
port: 3306
user: app_user
database: app_db
connect-timeout: 8
read-idle-timeout: 30
```

In `.authinfo.gpg`, use `machine` as the profile id and `db-host` for the real database host (since `auth-source` maps `machine` to `:host`):

```text
machine mysql/prod login app_user password db-password-here \
  backend mysql db-host db.example.com port 3306 database app_db
```

See the [auth-source manual](https://www.gnu.org/software/emacs/manual/html_node/auth/index.html) for other supported stores.

## SSH Tunnels via ~/.ssh/config

Adding `:ssh-host` opens a local OpenSSH port forward through that `~/.ssh/config` host alias before connecting, keeping `:host` and `:port` as seen from the bastion. Forwarding applies only to structured `:host` / `:port` entries; opaque `:url` profiles (JDBC or MongoDB URLs) need a manual tunnel.

When the same database can be reached either directly or via a bastion, define two entries sharing one `:profile-entry`:

```emacs-lisp
(setq clutch-connection-alist
      '(("prod-pg" . (:backend pg :profile-entry "pg/prod"))
        ("prod-pg-ssh" . (:backend pg :profile-entry "pg/prod"
                           :ssh-host "bastion-prod"))))
```

Tunnels run with `ssh -o BatchMode=yes` so they cannot prompt interactively. Make sure non-interactive auth works (for example via `ssh-agent` or `AddKeysToAgent`). If a host needs first-time host-key confirmation or a passphrase, run `M-x clutch-prepare-ssh-host` (or `S` in `C-c ?`) once.

By default, each Clutch tunnel uses its own SSH connection (ignoring `ControlMaster`) so disconnecting always closes the forwarded port. To reuse an existing `ControlMaster` connection (for hosts needing a password or 2FA, or `/rpc:` paths with `tramp-rpc`), set `clutch-ssh-isolate-connections` to `nil`. Note that shared connections keep their forwarded port open until the shared SSH session closes.

## TRAMP-aware Connection Origin

Clutch only checks TRAMP when opening a connection and stores the chosen origin for later reconnects and queries. Set `:tramp-default-directory` on a saved connection to always connect through a remote host or container:

```emacs-lisp
("remote-pg" . (:backend pg
                 :host "127.0.0.1" :port 55433
                 :user "app" :database "appdb"
                 :tramp-default-directory "/ssh:devbox:/workspace/"))
```

Supported TRAMP methods are SSH-like (`ssh`, `scp`, `rsync`, `rpc`) and containers (`docker`, `podman`, including multi-hop paths like `/ssh:devbox|docker:db:/`). Container paths relay traffic through `exec -i` and require `socat`, `nc`, `netcat`, or `bash` inside the container. SSH-like TRAMP paths use batch OpenSSH just like `:ssh-host`, so non-interactive SSH auth is required. Like `:ssh-host`, forwarding applies only to structured `:host` / `:port` entries.

When neither `:ssh-host` nor `:tramp-default-directory` is configured and you connect from a TRAMP buffer, `clutch-tramp-context-policy` decides whether to inherit that buffer's remote context: `nil` (never), `ask` (default), or `auto`.

## Working with SQL files

You do not have to use a query console; any `.sql` file works with `clutch-mode`:

```text
1. Open a .sql file
2. M-x clutch-mode   ;; inherits sql-mode highlighting
3. C-c C-e           ;; connect (saved or temporary)
4. C-c C-c           ;; run region or statement at point
```

To enable `clutch-mode` automatically for `.sql` files (`.mysql` files already do this automatically):

```emacs-lisp
(add-to-list 'auto-mode-alist '("\\.sql\\'" . clutch-mode))
```

In regular `.sql` or REPL buffers, `C-c C-e` opens the full connection picker; picking the same connection reconnects the session (keeping commit mode and results), while picking another connects just that buffer. Inside a dedicated query console, `C-c C-e` reconnects that console's own connection directly; use `M-x clutch-query-console` to switch to a different connection.

For troubleshooting, enable `M-x clutch-debug-mode`, reproduce the issue, and inspect `*clutch-debug*` (shows redacted trace events, generated internal SQL, and JDBC stderr/stack traces when available).

## Everyday Commands

Quick reference (full details and all shortcuts are in the [interactive client guide](interactive-client.md)):

- **Execute & navigate:** `C-c C-c` runs the region or statement at point; `C-c ?` opens the transient menu; `C-x o` / `C-x b` switches to results.
- **Completion & lookup:** `C-c TAB` completes SQL identifiers (including qualified `schema.` tables, `alias.` columns, and derived-table columns); `M-.` jumps from a SQL alias to its definition in the statement; `C-c C-j` / `C-c C-d` / `C-c C-o` browse, describe, and act on database objects.
- **Result grid:** `RET` opens vertical Record view; `{` / `}` jump to the first/last visible columns; `s` cycles sorting; `/` filters the current page; `C-s` / `C-r` isearch also finds column headers; `c` copies the current cell or selection; `e` exports all result rows; `k` (`M-x clutch-copy-context-for-agent`) copies SQL, table metadata, and a result sample as Markdown for an external AI agent.
- **Staged editing:** `C-c '` edits a cell or Record field (opening JSON values in a JSON editor automatically); `i` / `I` opens a new or cloned insert form; `d` stages row deletion; `C-c C-k` discards the staged change at point; `C-c C-c` previews and submits all staged changes.

## Timeouts, Interrupts, and Customization

On native MySQL, native PostgreSQL, and JDBC connections, queries run asynchronously without blocking Emacs, and `C-g` in the console, REPL, or result buffer cancels them; on other backends, queries block and `C-g` interrupts them. See [Query Timeout and Interrupt](interactive-client.md#query-timeout-and-interrupt) for timeouts, cancellation behavior, and all customization variables.

## Upgrading From Pre-Split Checkouts

If you are upgrading from an older checkout that bundled protocol clients and Org-Babel support inside Clutch:

- Install [`mysql`](https://melpa.org/#/mysql) ([mysql.el](https://github.com/LuciusChen/mysql.el)) for `:backend mysql`.
- Install [`pgsql`](https://melpa.org/#/pgsql) ([pgsql.el](https://github.com/LuciusChen/pgsql.el)) for `:backend pg` or `:backend xtdb`.
- Install [mongodb.el](https://github.com/LuciusChen/mongodb.el) for `:backend mongodb`.
- Install [redis.el](https://github.com/LuciusChen/redis.el) for `:backend redis`.
- Install [ob-clutch](https://github.com/LuciusChen/ob-clutch) for Org-Babel source blocks.
- Add an explicit `:backend` to every saved connection.
