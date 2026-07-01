# Changelog

## 0.2.0 - Unreleased

This section summarizes the planned 0.2.0 release relative to `origin/main`.

### Breaking Changes

- Native MySQL and PostgreSQL protocol packages are no longer installed through
  `clutch` package dependencies. Install `mysql.el` for `:backend mysql` and
  `pg.el` / `pg-el` for `:backend pg` in environments that do not already have
  those packages.
- Internal backend facade consumers must require `clutch-backend.el` instead of
  `clutch-db.el`. Custom backend adapters also need to follow the current
  backend contract, including `clutch-db-object-definition` rather than the old
  show-create-specific hooks.
- Generic JDBC URL configurations now require an explicit `:driver-class`.
  Built-in JDBC backend aliases such as Oracle, SQL Server, DB2, Snowflake,
  Redshift, and ClickHouse still provide their own driver classes.

### Added

- Added the native MongoDB backend as `:backend mongodb`.
  The default MongoDB surface uses the external `mongodb.el` package and a
  basic MongoDB Shell / MQL helper query buffer, not `mongosh`, JavaScript
  evaluation, or JDBC.
- Added MongoDB SQL Interface as a `:surface sql-interface` path on the same
  `mongodb` backend. This keeps MongoDB as one user-facing backend while still
  supporting Atlas / Enterprise Advanced SQL Interface endpoints through JDBC
  when that surface exists.
- Added `clutch-document.el` for document query-buffer behavior. It currently
  provides MongoDB helper syntax, highlighting, completion, statement
  boundaries, and document query-buffer dispatch.
- Added MongoDB object and metadata workflows:
  collection describe/profile output, object definition JSON, index insight,
  validation metadata, collection stats, sample explain, browse command
  generation, and database/collection schema refresh.
- Added MongoDB result-grid mapping for sampled document fields. Nested
  documents and arrays are treated as JSON-valued cells and native MongoDB
  result copy/export can generate helper snippets such as `insertOne`,
  `insertMany`, `replaceOne`, `updateOne`, and `deleteOne`.
- Added a basic Redis key/value backend as `:backend redis`, using the external
  `redis.el` RESP client. Redis supports command execution, key browsing,
  type-aware value display, TTL metadata, and result-grid mapping for common
  Redis data structures.
- Added backend support-level documentation that separates core SQL support,
  basic SQL/query-first support, basic document support, basic key/value
  support, and SQL Interface surfaces.
- Added architecture documentation with diagrams for module layering, backend
  surfaces, connection flow, query/result flow, object flow, and lazy optional
  dependency loading.

### Changed

- Renamed the generic database facade from `clutch-db.el` to
  `clutch-backend.el`. Workflow modules now route through the generic backend
  contract instead of depending on concrete protocol packages.
- Extended the backend registry with data-model, support-level, query-mode,
  surface, and normalization metadata. This keeps SQL, document, key/value, and
  JDBC surfaces explicit at the registry boundary.
- Moved backend-specific connection parameter normalization into backend
  registry entries instead of hard-coding concrete backend normalization inside
  the facade.
- Made optional protocol dependencies lazy and backend-specific. Native
  `mysql.el`, `pg.el`, `mongodb.el`, and `redis.el` are required only when the
  matching backend is used, and missing packages report connection-time errors.
- Refined result action ownership. `clutch-result.el` owns result state,
  paging/filter/sort/refine state, value/record workflows, and the result
  action registry; `clutch-ui.el` owns shared grid/header/footer rendering
  helpers; `clutch-edit.el` owns staged mutation state.
- Updated result-cell truncation so incomplete cell display uses a compact
  single-character ellipsis (`…`) instead of silently cutting text.
- Updated header-line shortcut hints so shortcut keys and their descriptions
  use distinct faces, matching the visual separation used by Transient.
- Made result column sorting a single three-state cycle. Pressing `s` on the
  current column or clicking a result column header now cycles unsorted,
  ascending, descending, then unsorted again; use `C` to jump to another visible
  column first. Headers show original-size neutral, ascending, and descending
  icons after column names.
- Made transient menus expose current operational state using highlighted
  choices. Auto-commit, copy refinement, filters, sorting, result layout, staged
  mutation counts, and Record field actions now update their labels from the
  active buffer context; unavailable stateful actions remain visible but inapt.
- Updated JSON result-cell display to match a DataGrip-like rule:
  short JSON is shown inline with lightweight token highlighting, while long
  JSON shows a compact prefix ending in `…` and remains unhighlighted.
- Kept binary BLOB cells compact with `<BLOB>` placeholders, while allowing
  small JDBC JSON/XML text BLOBs to use the normal compact text display path.
- Reworked live workflow test backend selection around a shared capability
  table so SQL-native and JDBC workflow coverage no longer depends on scattered
  hard-coded backend lists.
- Reworked MongoDB query-console completion around common helpers, collection
  names, sampled field names, aggregation stages/operators, and supported
  cursor helper chains.
- Clarified that DuckDB belongs to the SQL-first model and is currently reached
  through generic JDBC configuration.

### Fixed

- Fixed copy transient state labels so toggling `-r` follows the live switch
  value and immediately refreshes the highlighted `No|Yes` choice.
- Fixed result header sort icons so their graphical width stays aligned
  with the monospace result grid.
- Refined result header sort indicators and column-name underlining so spacing
  after the name stays visually clean.
- Prevented result column navigation and stale header clicks from silently
  targeting hidden or different columns during sorting.
- Reduced SQL first-query latency by stopping row-identity metadata lookup after
  the first usable candidate across MySQL, PostgreSQL, SQLite, and JDBC; MySQL
  unique-index fallback now uses scoped `SHOW KEYS` metadata.
- Delayed automatic schema cache refresh after connecting until Emacs has been
  idle for the configured delay, so native metadata refresh is less likely to
  occupy the foreground connection before the first query.
- Recover native MySQL connections after a client-side query read timeout by
  cancelling and draining the timed-out server query; when recovery fails, close
  the connection instead of letting later UI metadata requests reuse an
  unsynchronized protocol stream.
- Deferred automatic JDBC schema refresh until the configured idle window, so
  Oracle/JDBC metadata preheat does not start immediately after connecting.
- Updated the bundled JDBC agent pin to 0.2.6. JDBC metadata requests no longer
  block foreground execution, and small UTF-8/GB18030 JSON/XML BLOB values can
  render through the normal text/JSON/XML cell display path.
- Fixed result-grid rendering for JDBC JSON text stored in BLOB columns, so the
  cell shows a compact JSON prefix with `…` instead of falling back to `<BLOB>`.
- Throttled repeated result-grid column width changes, so holding `=` or `-`
  updates the width state continuously while limiting full table redraws and
  skipping cursor-only header/footer refresh work.  Reduced the default width
  step so manual column resizing feels smoother.
- Kept SQL expression completion scoped to columns, so `WHERE` and similar
  clauses no longer include statement table names as identifier candidates.
- Kept point on the edited result cell after staging or cancelling a cell edit,
  instead of returning to the start of the result table.
- Preserved clear boundary errors for unsupported MongoDB helper syntax instead
  of passing unsupported shell-only constructs to an external process.
- Improved MongoDB metadata buffers so JSON object definitions, collection
  profiles, stats, validation, and explain output are rendered as formatted JSON
  instead of table-style describe output.
- Fixed row-identity metadata error visibility so MySQL, PostgreSQL, SQLite,
  and JDBC adapter lookup failures are surfaced instead of being
  indistinguishable from absent metadata.
- Improved object action availability and presentation so MongoDB-only actions
  are enabled only for MongoDB collection entries and do not leak into SQL
  object buffers.
- Kept native document and key/value surfaces out of SQL-only staged mutation,
  manual transaction, row-identity edit, and SQL rewrite workflows.
- Made JDBC connections send an explicit driver class to the sidecar. This
  prevents unrelated registered JDBC drivers from claiming the same URL prefix;
  generic `:backend jdbc` connections now require `:driver-class`.

### Documentation

- Added `docs/mongodb-backend.org` with MongoDB native and SQL Interface
  requirements, configuration examples, query-buffer behavior, supported helper
  families, Clutch concept mapping, object actions, and live test notes.
- Added `docs/backend-support.org` to document support levels and boundaries
  for MongoDB, MongoDB SQL Interface, DuckDB, Redis, and future database
  categories.
- Added `docs/architecture.md` as the current module and backend architecture
  map.
- Updated README and existing backend docs for MongoDB, Redis, JDBC driver
  setup, optional protocol packages, and support-level terminology.
- Added postmortem records for MongoDB JDBC boundaries, query-mode facets,
  object actions, validation/stats actions, cursor-chain completion, document
  result capability gates, Redis basic support, metadata error visibility, and
  result action ownership.
- Updated `AGENTS.md` with architecture, optional dependency, MongoDB,
  protocol-package, and refactoring guardrails for future work.

### Tests

- Split the large test suite into more focused files for connection, debug,
  live workflow, object workflow, backend behavior, and shared helpers.
- Added native MongoDB unit coverage for helper parsing, BSON constructor
  handling, query result mapping, schema sampling, object metadata, collection
  actions, SQL Interface routing, and error translation.
- Added Redis backend tests for key scanning, key metadata, type-aware browse
  queries, command result mapping, and live connection/query/schema behavior.
- Extended the live test runner to cover PostgreSQL, MySQL, MongoDB, and Redis
  containers under the native live test workflow.
- Added result rendering tests for JSON inline display, JSON truncation,
  compact ellipsis behavior, custom column displayers, and BLOB compact width.

### Compatibility Notes

- MongoDB user configuration should use `:backend mongodb`.
  `:backend mongodb :surface sql-interface` is the only MongoDB SQL Interface
  configuration path.
- Native MongoDB requires the external `mongodb.el` package. MongoDB SQL
  Interface requires the JDBC sidecar and MongoDB JDBC driver jar.
- Redis requires the external `redis.el` package.
- Native MongoDB and Redis support are intentionally basic and do not expose
  SQL row editing, joins, SQL transaction UI, SQL row identity editing, or
  SQL-only rewrite workflows.
