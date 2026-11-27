# clutch Backend Support Levels

## Support Levels

Clutch groups backends by what workflows they can actually support, not just whether they can open a connection:

| Level | Meaning | Examples |
|---|---|---|
| Core SQL | Full SQL workflow: queries, metadata, result grid, completion, pagination, and staged row edits where row identity and transactions exist. | MySQL, PostgreSQL, SQLite, Oracle, SQL Server |
| Core SQL (generic JDBC entry) | Same SQL workflow, configured via a generic `:backend jdbc` entry instead of a dedicated backend prompt. | DuckDB |
| Basic SQL | Query and metadata support; editing, transactions, row identity, or dialect coverage may be partial. | ClickHouse, Snowflake, Redshift, DB2, generic JDBC, XTDB (via `pgsql`) |
| Basic document | Native document queries and read-only grids without SQL row editing. | MongoDB (`:backend mongodb`) |
| Basic key/value | Command-oriented console, key browsing, and type-aware value display without SQL editing or joins. | Redis (`:backend redis`) |
| SQL Interface | SQL/JDBC endpoint for a non-relational database that exposes one. | MongoDB SQL Interface (`:backend mongodb :surface sql-interface`) |
| Basic CQL | CQL console, table/column browsing, and bounded read-only previews. | Cassandra (via JDBC) |
| Unsupported | Needs a different backend/UI model. | Elasticsearch, Couchbase |

## MongoDB

`:backend mongodb` connects to standard `mongod` deployments via the external `mongodb.el` client and accepts common MQL helper syntax, with no Java or JDBC jar required. See the `mongodb.el` repo for protocol options.

For Atlas or Enterprise deployments that expose MongoDB SQL Interface, add `:surface sql-interface` to the same `:backend mongodb` entry (requires the JDBC sidecar and MongoDB JDBC driver).

## DuckDB

DuckDB fits Clutch's SQL workflow and connects via `:backend jdbc` with a file-backed `jdbc:duckdb:...` URL and the auto-installable DuckDB JDBC driver:

- `clutch-switch-schema` lists and switches schemas inside the file-backed catalog only. Pure in-memory URLs and `ATTACH`ed catalogs live only in the primary JDBC session, which Clutch's separate metadata session cannot see. Query attached objects with qualified `catalog.schema.table` names.
- The current DuckDB JDBC driver has no savepoint support, so Manual-mode staged submission is rejected; switch to Auto mode to submit staged edits.

## Redis

Redis uses `:backend redis` and the external `redis.el` client for line-oriented command buffers, key browsing, TTL/existence metadata, and type-aware values. It does not support SQL editing, transactions, pub/sub loops, clusters, or stream consumers.

- Saved profiles can set an initial `:database`, and running `SELECT` in the console moves the session and survives idle reconnects (`C-c C-e` returns to the profile's database). Interactive schema switching is disabled because Redis has no ACL-safe way to enumerate database counts.
- Key discovery is capped at `clutch-redis-key-discovery-limit` (default 5000 keys, at most 8 `SCAN` batches); generated hash/list/set/zset/stream browse commands request at most `clutch-redis-browse-limit` elements (default 200). Typing an exact key beyond the snapshot performs a single existence check.
- Generated hash browsing uses `HRANDFIELD` and requires Redis 6.2+. Manual `SCAN` commands work everywhere and display continuation cursors on empty nonterminal batches.
