# clutch JDBC Backend Guide

`clutch-db-jdbc.el` connects to databases without a pure-Elisp client by talking to [clutch-jdbc-agent](https://github.com/LuciusChen/clutch-jdbc-agent), a small JVM sidecar (requires Java 17+). For wire protocol and session internals, see [jdbc-agent-protocol.md](jdbc-agent-protocol.md).

## Supported Databases

| Backend symbol | Database | Support level | Driver install | Default port |
|---|---|---|---|---|
| `oracle` | Oracle Database | Core SQL | Auto (Maven Central) | 1521 |
| `sqlserver` | Microsoft SQL Server | Core SQL | Auto (Maven Central) | 1433 |
| `redshift` | Amazon Redshift | Basic SQL | Auto (Maven Central) | 5439 |
| `snowflake` | Snowflake | Basic SQL | Auto (Maven Central) | 443 |
| `clickhouse` | ClickHouse | Basic SQL | Auto (Maven Central) | 8123 |
| `mongodb` (`:surface sql-interface`) | MongoDB SQL Interface | SQL Interface | Auto (Maven Central) | 27017 |
| `cassandra` | Apache Cassandra | Basic CQL | Manual download | 9042 |
| `db2` | IBM DB2 | Basic SQL | Manual download | 50000 |

DuckDB, TiDB, Hive, KingbaseES, and other JDBC databases work through `:backend jdbc` with `:url`, `:driver-class`, and an optional `:display-name` (place the driver jar in the drivers folder, or run `clutch-jdbc-install-driver 'duckdb` for DuckDB).

## Setup

On first connect, Clutch checks for the agent jar and driver jar and prompts you if either is missing. You can also install them up front:

```emacs-lisp
(clutch-jdbc-ensure-agent)                ; downloads clutch-jdbc-agent.jar from GitHub Releases

(clutch-jdbc-install-driver 'oracle)      ; ojdbc8.jar + orai18n.jar (recommended for 11g/12c/19c)
(clutch-jdbc-install-driver 'oracle-11)   ; ojdbc11.jar + orai18n.jar (19c+ only)
(clutch-jdbc-install-driver 'sqlserver)   ; mssql-jdbc.jar (JRE-classified artifact)
(clutch-jdbc-install-driver 'snowflake)   ; snowflake-jdbc.jar
(clutch-jdbc-install-driver 'redshift)    ; redshift-jdbc42.jar
(clutch-jdbc-install-driver 'clickhouse)  ; clickhouse-jdbc.jar + SLF4J jars
(clutch-jdbc-install-driver 'duckdb)      ; duckdb_jdbc.jar
(clutch-jdbc-install-driver 'mongodb)     ; MongoDB SQL Interface JDBC driver
;; DB2: download db2jcc4.jar from IBM manually into ~/.emacs.d/clutch-jdbc/drivers/
```

Jars live in `~/.emacs.d/clutch-jdbc/drivers/` (customizable via `clutch-jdbc-agent-dir`). Reconnecting restarts the shared agent automatically if you install a new driver while it is running, so no Emacs restart is needed.

- **Agent integrity:** Clutch verifies `clutch-jdbc-agent.jar` against `clutch-jdbc-agent-sha256` before launch and removes older versioned agent jars on `clutch-jdbc-ensure-agent`. Set `clutch-jdbc-agent-sha256` to `nil` when testing a locally built jar.
- **Oracle & SQL Server jar notes:** Installing `oracle` removes `ojdbc11.jar` and installing `oracle-11` removes `ojdbc8.jar` so both lines are never loaded together; both include `orai18n.jar` for CJK character sets (e.g. `ZHS16GBK`). SQL Server downloads Microsoft's JRE-classified artifact (such as `mssql-jdbc-13.4.0.jre11.jar`) and saves it as `mssql-jdbc.jar`.
- **Idle liveness check:** After `clutch-jdbc-validate-after-idle-seconds` (default 300s; `nil` or `0` disables), the agent runs `Connection.isValid` before preparing a new statement. If dead before execution starts, Clutch reconnects and runs the statement once on auto-commit or clean manual sessions (dirty manual transactions and in-flight failures are never replayed). Idle metadata/bulk sessions are replaced independently without disturbing the primary session.

## Agent Launch Options

- `clutch-jdbc-agent-java-executable`: Java binary (default `"java"`).
- `clutch-jdbc-agent-jvm-args`: Extra JVM flags (default `("-Xss512k" "-Dpolyglot.engine.WarnInterpreterOnly=false")`).

The agent reserves stdout strictly for JSON protocol traffic and redirects driver `System.out` logs (such as Snowflake SSO messages) to stderr so Clutch diagnostics can capture them safely.

## Connection Examples

```emacs-lisp
(setq clutch-connection-alist
      '(;; Oracle (service name)
        ("prod-oracle" . (:backend oracle
                           :host "db.corp.com" :port 1521
                           :database "ORCL"
                           :user "scott" :pass-entry "prod-oracle"
                           :connect-timeout 8 :read-idle-timeout 30
                           :query-timeout 20 :rpc-timeout 25))

        ;; Oracle (SID)
        ("prod-oracle-sid" . (:backend oracle
                               :host "db.corp.com" :port 1521
                               :sid "ORCL"
                               :user "scott" :pass-entry "prod-oracle"))

        ;; SQL Server
        ("prod-mssql" . (:backend sqlserver
                          :host "sqlserver.corp.com" :port 1433
                          :database "AdventureWorks"
                          :user "sa" :pass-entry "prod-mssql"
                          :connect-timeout 8 :query-timeout 15))

        ;; Redshift
        ("prod-redshift" . (:backend redshift
                             :host "cluster.abc123.us-east-1.redshift.amazonaws.com"
                             :port 5439 :database "analytics"
                             :user "awsuser" :pass-entry "prod-redshift"))

        ;; Snowflake
        ("prod-snowflake" . (:backend snowflake
                              :host "myorg-myaccount"
                              :database "MYDB"
                              :user "myuser" :pass-entry "prod-snowflake"))

        ;; ClickHouse
        ("clickhouse" . (:backend clickhouse
                          :host "ch.corp.com"
                          :database "default"
                          :user "default" :password ""))

        ;; MongoDB SQL Interface
        ("atlas-sql" . (:backend mongodb
                         :surface sql-interface
                         :host "cluster0.a.query.mongodb.net"
                         :database "analytics"
                         :auth-database "admin"
                         :user "reporter" :pass-entry "atlas-sql"))

        ;; DuckDB (file-backed via generic JDBC)
        ("duckdb" . (:backend jdbc
                      :url "jdbc:duckdb:/path/to/analytics.duckdb"
                      :driver-class "org.duckdb.DuckDBDriver"
                      :display-name "DuckDB"))

        ;; Generic JDBC (for databases without a dedicated backend)
        ("kingbase" . (:backend jdbc
                        :url "jdbc:kingbase8://db.corp.com:54321/test"
                        :driver-class "com.kingbase8.Driver"
                        :display-name "KingbaseES"
                        :user "system" :pass-entry "kingbase"
                        :rpc-timeout 15))))
```

Credentials follow [Password Management](user-guide.md#password-management).

## Database-Specific Notes

### Oracle

- **Driver choice:** Default `ojdbc8` works safely across 11g/12c/19c (avoids `ojdbc11` LOB bugs on 11g). Use `oracle-11` only if you are exclusively on 19c+.
- **SID vs service name:** Use `:sid` for `@host:port:SID` and `:database` for `@//host:port/service`; mixing them up hangs on connect.
- **Initial schema:** Defaults to the upper-cased username unless `:schema` is set (`:schema "\"Reports\""` preserves mixed case for SQL execution, though the agent's catalog lookup uses upper-case names).
- **Fast completion & low-privilege accounts:** Completion uses prefix queries instead of blocking connect on a full schema scan, while a background refresh fills `C-c C-j` with current-schema tables/views, `user_synonyms`, and accessible `all_tables`/`all_views` (excluding system owners; `PUBLIC SYNONYM` dictionary views participate in completion without cluttering `C-c C-j`).
- **Cancellation, pagination, dates, and CLOBs:** Cancellation runs in-band by default (set `:props (("oracle.net.disableOob" . "false"))` for OOB). Pagination uses `ROWNUM` subqueries (9i+ compatible). `DATE` preserves time whenever the driver returns `Timestamp`. CLOBs return up to 256 UTF-16 code units; previews shorter than the full length stay read-only to prevent data loss (complete short CLOBs edit normally).

### Cassandra

Download `cassandra-jdbc-wrapper-5.0.5-bundle.jar` from the [ING JDBC wrapper release](https://github.com/ing-bank/cassandra-jdbc-wrapper/releases/tag/v5.0.5) into the drivers directory (BLOB results need agent 0.2.29+):

```emacs-lisp
("cassandra-local" . (:backend cassandra
                     :url "jdbc:cassandra://127.0.0.1:9042/my_keyspace?localdatacenter=datacenter1"))
```

Set the initial keyspace in `:url` and qualify other keyspaces as `keyspace.table` (`USE` and schema switching are unavailable). Unbounded `SELECT` queries get an automatic CQL `LIMIT` (`clutch-jdbc-fetch-size` + 1, max 9999 rows) so the driver does not materialize entire tables; results are read-only previews without server-side pagination, staged editing, or DDL/comparison actions. Reconnect after DDL to refresh the driver's schema snapshot.

### Snowflake

Each logical Clutch connection opens two JDBC sessions (foreground + metadata), so interactive authenticators like `externalbrowser` or MFA can prompt twice per connect. To cache tokens, an `ACCOUNTADMIN` can enable `ALTER ACCOUNT SET ALLOW_ID_TOKEN = TRUE;` and you can drop the matching JNA jar(s) into `drivers/` (Snowflake JDBC 3.12.18+ no longer bundles JNA). See Snowflake's [connection-caching](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use#using-connection-caching-to-minimize-the-number-of-prompts-for-authentication-optional) and [JNA classpath](https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-download#add-the-jna-classes-to-your-classpath) docs.

### ClickHouse

For `:url` entries, Clutch reads the database from the last `database` property or path segment (decoding `%XX` escapes once and `+` as space; use `%2B` for literal `+`). `clutch-switch-schema` updates the URL and keeps its path and properties.

### DuckDB

`C-c C-l` lists and switches schemas inside the file-backed catalog via `USE catalog.schema`. In-memory databases and `ATTACH`ed catalogs stay local to the primary session and cannot be shared with the metadata session (or restored automatically on reconnect), so use qualified `catalog.schema.table` names for attached objects.

### MongoDB SQL Interface Notes

Use `:backend mongodb :surface sql-interface` only for Atlas or Enterprise endpoints exposing `jdbc:mongodb://...` (ordinary `mongod` uses native `mongodb.el`; see [mongodb-backend.md](mongodb-backend.md)). Clutch passes `:database` as the required JDBC `database` property, maps `:auth-database`/`:auth-source` to the URL auth path, paginates with `LIMIT`/`OFFSET`, and omits manual transaction controls.

Live tests (`:sql-interface-mongodb-live`) require a real SQL Interface endpoint:

```sh
emacs -Q --batch -L . -L test -l ert -l clutch-db-test \
  --eval '(setq clutch-db-test-sql-interface-mongodb-url "jdbc:mongodb://cluster0.a.query.mongodb.net/admin"
                clutch-db-test-sql-interface-mongodb-database "analytics"
                clutch-db-test-sql-interface-mongodb-user "reporter"
                clutch-db-test-sql-interface-mongodb-password "secret")' \
  --eval "(ert-run-tests-batch-and-exit '(tag :sql-interface-mongodb-live))"
```

## Row Identity, Plans, and Transactions

- **Staged edits:** Row identity checks PK → non-null unique keys → `ROWID` on confirmed Oracle base tables (Oracle views/synonyms stay read-only). Schema-qualified queries keep their schema for both metadata lookup and DML generation.
- **Query plans:** `clutch-explain-plan` (`E` in `C-c ?`) works over JDBC when `:url` starts with `jdbc:postgresql:` or `jdbc:mysql:` (except MariaDB, which lacks tree plans); other JDBC backends report plans unavailable.
- **Transactions:** Oracle defaults to manual commit (`clutch-jdbc-oracle-manual-commit`, overridable per connection with `:manual-commit t/nil`); other JDBC backends default to auto-commit. Shared `C-c C-a`/`C-c C-m`/`C-c C-u` and savepoint behavior are described in [Control transactions](user-guide.md#4-control-transactions). Note that Oracle DDL (`CREATE`/`ALTER`/`DROP`/`TRUNCATE`/`RENAME`) auto-commits even in manual mode (`ALTER SESSION`/`ALTER SYSTEM` do not). Open DML result buffers show status banners (`✓ Transaction committed`, `⚠ Transaction rolled back — changes not persisted`, `✕ Connection closed`) until reused by the next query.

## Additional JDBC Connection Properties

Pass arbitrary driver properties via `:props`:

```emacs-lisp
("mssql-encrypt" . (:backend sqlserver
                     :host "sqlserver.corp.com" :port 1433
                     :database "mydb" :user "sa" :password "secret"
                     :props (("encrypt" . "true")
                             ("trustServerCertificate" . "true"))))
```
