# Backend Support Levels And Transport Boundaries

## Context

MongoDB made the old "full SQL support" label misleading. Clutch has a strong
SQL path, but it is not trying to claim DataGrip-level completeness for every
server feature, dialect extension, admin tool, or visual workflow.

At the same time, MongoDB needed to share existing SSH/TRAMP transport behavior
without pulling MongoDB URI parsing, SRV semantics, or driver-specific endpoint
rules into Clutch.

## Decision

Use `core` for the primary relational SQL path. MySQL, PostgreSQL, SQLite,
Oracle, and SQL Server are core SQL support because Clutch targets the normal
query, object, result-grid, completion, pagination, and staged mutation
workflows for them.

Keep `basic` for query-first or less integrated surfaces. Generic JDBC,
ClickHouse, Snowflake, Redshift, DB2, and native MongoDB remain basic support
because Clutch exposes useful query/object workflows but does not claim the
same editing, transaction, dialect, or data-model completeness.

Keep MongoDB SQL Interface as a surface of `mongodb`, not as another backend.
It is a SQL/JDBC endpoint for deployments that actually provide MongoDB SQL
Interface, while ordinary MongoDB uses the native document surface through the
external `mongodb.el` package.

Transport stays below the backend data model. SSH tunnels and TRAMP forwarding
can be reused by MongoDB only when the saved connection has structured `:host`
and `:port` params. Clutch rewrites those TCP endpoint params to the local
forward and then calls the backend normally.

Clutch does not rewrite `mongodb://` or `mongodb+srv://` URLs for SSH/TRAMP.
Those URLs are opaque connection params owned by `mongodb.el`; parsing them in
Clutch would duplicate driver behavior and make SRV/TLS/effective-database rules
drift across packages.

## Consequences

The UI can show no annotation for core SQL backends and mark basic
backends honestly in completion prompts. The support contract no longer implies
"full database product coverage" where Clutch only owns a focused client
workflow.

Future document backends can reuse transport if they expose a simple host/port
TCP endpoint. If a backend only exposes an opaque URL, Clutch should either pass
it through directly or require a public protocol-package API for transport
overrides instead of parsing the URL itself.

Future JDBC-backed features should be described as SQL surfaces unless they are
promoted into the core SQL workflow contract with tests covering the shared
object/result/editing paths.
