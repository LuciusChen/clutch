# clutch — Product Requirements

## Purpose

Clutch is a keyboard-first database client for Emacs. It should let users connect, inspect metadata, execute queries or native commands, review results, and perform supported mutations without leaving their editor.

The product is for developers, data engineers, analysts, and researchers who prefer an Emacs-native workflow over a separate GUI. It is not intended to reproduce every database vendor's administration console or shell.

## Product principles

- Keep the common path direct: connect, discover an object, run a query, inspect the result, and act on it.
- Expose only workflows that a backend can support honestly; capability gates must prevent SQL-only actions from leaking onto document or key/value surfaces.
- Prefer one consistent result and object workflow over backend-specific user interfaces, while preserving backend-specific query languages where the data model requires them.
- Make destructive operations staged, inspectable, and fail-closed.
- Keep optional protocol clients and the JDBC sidecar behind explicit backend boundaries and load them only when needed.
- Bound interactive and protocol work so metadata refreshes, large responses, or unbounded discovery do not freeze Emacs.

## Supported surfaces

The authoritative support-level definitions and current backend matrix live in [`docs/backend-support.org`](docs/backend-support.org). User configuration and installation instructions live in [`README.org`](README.org).

| Surface | Product requirement |
|---|---|
| Core relational SQL | MySQL, PostgreSQL, SQLite, Oracle, and SQL Server should provide query execution, metadata discovery, result browsing, completion, pagination, and staged mutations when row identity and transaction semantics are available. |
| Generic or query-first JDBC | DuckDB, ClickHouse, Snowflake, Redshift, DB2, and generic JDBC URLs should provide reliable query and metadata workflows without claiming unsupported mutation or dialect behavior. |
| Native MongoDB | The `mongodb` backend should provide a bounded, read-oriented document workflow through the external `mongodb.el` client and a documented subset of MongoDB helper syntax. |
| MongoDB SQL Interface | `:backend mongodb :surface sql-interface` should use JDBC only when the target deployment exposes that SQL endpoint; it is not a second MongoDB backend. |
| Native Redis | The `redis` backend should provide bounded key discovery, command buffers, key browsing, TTL metadata, and type-aware values through the external `redis.el` client. |

## Required user workflows

### Connection and discovery

Users must be able to save connection parameters, resolve secrets through supported credential sources, connect directly or through supported SSH/TRAMP forwarding, reconnect, switch a database or schema when the backend permits it, and see connection and transaction state in attached buffers.

Object discovery must use backend metadata, remain bounded, and distinguish current, loading, and stale cache state. Two live connections with the same display label must not share connection-scoped metadata or refresh each other's buffers.

### Query and command consoles

Relational and SQL Interface buffers must provide SQL editing, syntax highlighting, statement execution, completion, Eldoc, and result navigation. Native MongoDB and Redis buffers must use their own documented command syntax instead of pretending to be SQL.

Query cancellation and timeout handling must leave the connection reusable only when the backend can establish that recovery is safe. Errors must retain enough redacted diagnostic context to explain the failing operation without exposing credentials.

### Results and object views

Results must support keyboard navigation, record/value views, bounded paging, sorting and filtering where semantically safe, copying/export, and context export. Rendering should use cached structured data rather than reparsing displayed text.

Describe and object-definition views must use the generic backend contract and degrade clearly when a capability is unavailable. Presentation refreshes must not own connection or metadata policy.

### Mutations

Supported relational backends must provide a single staged edit, insert, and delete vocabulary. Users must be able to inspect the execution preview, discard pending changes, and commit only after validation and confirmation.

Mutation identity must be stable across lookup, rendering, preview, and commit. Local validation must happen before the editing context is destroyed, and missing identity, table provenance, affected-row guarantees, or backend capability must stop execution rather than fall back to an unsafe guess.

SQL mutation execution must bind values through the backend contract, including JDBC `execute-params`; readable previews may render literals, but preview formatting is not an execution fallback.

## Architecture requirements

[`docs/architecture.md`](docs/architecture.md) is the source of truth for current module ownership and dependency direction. [`AGENTS.md`](AGENTS.md) defines contributor guardrails and mandatory checks.

The package entry point must remain a one-way composition root. Workflow modules depend on `clutch-backend.el`; backend adapters own database-specific integration; external protocol packages own wire protocols, authentication, sessions, and transport details.

Connection identity, not a display label, owns connection-scoped lifetime. Mutable state and public options belong with the workflow that owns their lifecycle, except for a genuinely shared contract whose placement would otherwise create more glue than it removes.

Mandatory dependencies must be represented by `require`; declarations are reserved for intentional lazy, optional, or cyclic boundaries. Architecture checks must reject stale declarations, dependency regressions, composition-root re-entry, and growth beyond the recorded graph budgets.

Diagnostics must remain a leaf over the backend contract. It stores redacted problem provenance and must not depend on connection callbacks, UI state, protocol-private APIs, or the composition root.

## Performance and reliability requirements

- Interactive paths must avoid work proportional to the complete remote dataset when a bounded page, sample, or key snapshot is sufficient.
- JDBC requests and responses must be framed incrementally, enforce configured bounds, and avoid per-row or per-chunk orchestration layers that do not remove measurable work.
- Metadata work must not contend with foreground JDBC execution; reconnect and schema switching must preserve one coherent logical connection context.
- Optional backend failures must surface at connection time with actionable errors and must not affect users of other backends.
- Public workflows and architecture invariants require focused tests through real dispatch paths; tests must not grow by duplicating implementation-specific mock harnesses.

## Compatibility and release requirements

Clutch targets Emacs 29.1 or newer. The JDBC sidecar targets Java 17 or newer. Raising either baseline requires an explicit release note, documentation update, and design rationale.

Changes to user-visible workflows, defaults, supported backends, public configuration, or dependencies must update `README.org` and `CHANGELOG.md` in the same release change. The pinned JDBC agent version and SHA-256 must describe the exact published jar bytes.

## Non-goals

- Full database administration, migration management, monitoring, or vendor-console parity.
- Full `mongosh` or arbitrary JavaScript compatibility in the MongoDB query adapter.
- Redis cluster administration, pub/sub loops, or stream-consumer management.
- Pretending query-first JDBC integrations have core mutation support before their identity, dialect, and transaction contracts are verified.
- Frameworks, registries, accessors, or state containers introduced only to improve a metric or hide a direct and stable dependency.

## Acceptance

A release is acceptable when its documented workflows match executable behavior, all mandatory CI and architecture checks pass, external protocol boundaries remain intact, and no known user-visible regression is hidden by a fallback or by tests that bypass the public path.
