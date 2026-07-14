# Changelog

## 0.3.0 - Unreleased

### Breaking Changes

- Removed the public `clutch-execute-query-at-point` and
  `clutch-execute-statement-at-point` commands.  Use
  `clutch-execute-dwim`, `clutch-execute-region`, or `clutch-execute-buffer`;
  select a region first when exact execution boundaries matter.
- Removed the table-specific `clutch-describe-table`,
  `clutch-describe-table-at-point`, and `clutch-browse-table` commands.  Use
  `clutch-describe-dwim` or `clutch-act-dwim`.
- Removed the standalone `clutch-result-insert-mode` entry point and its public
  map/hook.  Open insert forms with `clutch-result-insert-row` from a result.
- Removed the `:tramp` saved-connection parameter spelling.  Use
  `:tramp-default-directory` for explicit TRAMP connection origins.
- Reduced the native MongoDB console to common reads, generated single-document
  mutations, `runCommand`, and `ObjectId` / `ISODate`.  Database switching now
  uses `clutch-switch-schema`; dedicated admin/index helpers, database
  aggregation, multi-document mutations, cursor `batchSize` / `comment`, and
  numeric/timestamp constructor aliases were removed.

### Fixed

- Kept connection chrome synchronized across transaction changes and
  connection loss: headers detect asynchronously closed backends, attached
  result footers update immediately without stale transaction state, failed
  query interruption invalidates derived buffers, and DML outcome banners
  remain intact.
- Isolated schema, table metadata, async refresh, and object warmup state by
  live connection identity, so simultaneous connections with the same display
  label no longer share caches or refresh each other's result buffers, and
  retired connections are not retained by warmup freshness bookkeeping.
- Rejected Oracle URLs configured through generic `:backend jdbc` before
  connection setup, with guidance to use the Oracle backend and its SQL dialect.
- Prevented point-local DWIM execution from sending detached `--` divider
  paragraphs before the semicolon-delimited SQL at point.
- Removed hidden connection-construction retries and broad capability
  fallbacks, so backend failures reach the normal command error boundary with
  their original cause.
- Replaced PostgreSQL primary-key ordering through
  `array_position(int2vector, smallint)` with explicit array subscripts over
  `pg_index.indkey`, restoring row identity and result editing on GaussDB in
  PostgreSQL compatibility mode while preserving composite-key order.

## 0.2.4 - 2026-07-10

### Changed

- Updated the bundled JDBC agent pin to 0.2.8. JDBC staged mutations now bind
  positional values through `PreparedStatement` instead of rendering literals
  into SQL.

### Fixed

- Recovered an idle-timed-out JDBC metadata session independently, restoring
  its schema and retrying the metadata request once without replacing the
  healthy primary session or its transaction state.
- Encoded JDBC boolean parameters as JSON booleans, including false autocommit,
  and made the agent reject non-boolean protocol values.
- Preserved catalog, schema, and table components from qualified JDBC source
  names when resolving row identity, including delimited SQL Server names.
- Surfaced native MySQL, PostgreSQL, and SQLite metadata failures through the
  shared backend error boundary; synchronous Eldoc lookups remain quiet while
  recording recoverable warnings for diagnostics.

## 0.2.3 - 2026-07-10

### Added

- Added `:profile-entry` saved-connection profiles so encrypted pass or
  `.authinfo.gpg` entries can provide connection metadata while explicit
  `clutch-connection-alist` keys override profile defaults.

### Changed

- Updated the bundled JDBC agent pin to 0.2.7, adding generic JDBC table
  remarks while retaining checksum verification against the published jar.
- Unified header-line shortcut hints around status-first text followed by
  colored key/action pairs.
- Result headers now fall back to built-in text sort indicators (`↕`, `↑`,
  `↓`) when `nerd-icons` is unavailable, so sortable columns remain visible.

### Fixed

- Finished failed background schema refreshes when the connection closes, so
  query consoles no longer remain stuck at `[schema...]` after a MySQL metadata
  query hits a closed process.
- Prevented Query Console Eldoc from looping indefinitely when resolving table
  aliases in a non-final UNION branch.
- Allowed sorting UNION, grouped, derived, and other non-rewritable query
  results by falling back to a stable client-side sort of the current page.
- Reduced result-grid render work for wide pages by precomputing visible column
  metadata, avoiding repeated row-identity extraction while rendering staged
  state, and fast-pathing ordinary non-JSON/XML cell text.
- Extended graphical font-metric detection to Japanese and Korean fallback
  glyphs so mixed CJK result grids enable pixel alignment when those scripts
  do not match Emacs logical cell widths.
- Kept the current result cell selected when clicking the empty window area
  below the rendered table, while preserving normal cell clicks and mouse drag
  selection.
- Kept the result cursor on the last rendered row when mouse-wheel scrolling to
  the bottom of the table.
- Displayed database NULL as `<null>` in value viewers to match result and
  record cells.
- Opened REPL `SELECT` results in the standard result buffer instead of
  expanding wide tables inline in the REPL history, and styled REPL prompts,
  errors, and execution summaries for clearer command history.
- Recreated the REPL's dummy comint process before sending input or printing
  output so `RET` keeps working after the process disappears.
- Kept semicolons and parameter markers inside quoted SQL identifiers from being
  parsed as statement boundaries or placeholders, including doubled delimiter
  escapes in MySQL backticks and SQL Server brackets.
- Routed CTE-prefixed `UPDATE`, `DELETE`, and `INSERT` statements through the
  DML path instead of treating every `WITH` statement as a pageable query.
- Reported delimiter-only query buffers as empty input instead of dispatching an
  invalid internal query.
- Avoided broad auth-source password lookups when connection params have no
  host/user/port target, and avoided resolving saved connection passwords twice
  during interactive connect.
- Let explicit saved-connection `:backend` values guide backend-specific
  parsing for `:profile-entry` fields, so profiles can omit `backend` while
  keeping typed values such as Redis database numbers.
- Kept query consoles with the same display name but different connection
  identities in separate buffers instead of overwriting the existing console.
- Kept malformed JSON-looking text values in the plain value viewer instead of
  routing them to the JSON viewer.
- Copied Org tables with display-width alignment and right-aligned numeric
  columns.
- Required JSON edit sub-editors to start from valid JSON text, keeping invalid
  insert/edit buffer contents in place with a field-specific error.
- Rendered PostgreSQL array mutation parameters as curly-brace array literals, so
  editing array cells no longer sends JSON-style `[ ... ]` text as a string.
- Rejected stale edit buffers when the target row or original cell value changes
  before the edit is finished, avoiding staged updates against replaced results.
- Kept record view, region TSV copy, aggregate, edit/re-edit, and clone-to-insert
  actions aligned with the currently visible filtered rows, including filters
  that match no rows.
- Centered the target column in the result window when jumping by column name.
- Preserved the result buffer viewport when returning from cell edit buffers
  and when query-result refreshes restore the current cell.
- Rendered SQL NULL values in record view with the same `<null>` placeholder
  used by result cells.
- Allowed `C-c '` to edit fields from record view and refreshed the record view
  after staging the field edit.
- Avoided staging unchanged cell edits whose editable text still matches the
  original value, including numeric cells.
- Allowed `C-c C-k` in record view to discard the staged change at the current
  field.
- Removed the redundant `Field : Value` heading from record view.
- Cleared SQL WHERE-filter state when a new query replaces the result, avoiding
  stale filters in rerun, sort, and footer state.
- Escaped CSV column names and carriage returns using the same rules as result
  values so exported records remain structurally valid.
- Required native MongoDB `deleteOne` and `deleteMany` helpers to receive an
  explicit filter document instead of treating missing filters as `{}`.
- Rejected native MongoDB `insertOne` and `insertMany` helper calls whose
  payloads are not document values.
- Reduced large-schema completion and object discovery spikes by scanning query
  buffers once for referenced table identifiers and grouping object-cache
  entries without repeated list appends.
- Reduced Query Console Eldoc table-comment latency by priming comments from
  table discovery for MySQL/PostgreSQL and accepting optional JDBC table-entry
  comments when the agent supplies them.
- Reduced wide result-grid TAB navigation latency by caching normalized header
  sort indicators and reusing computed column widths during horizontal
  visibility checks.
- Avoided synchronous foreign-key and insert-placeholder metadata loads while
  rendering result buffers, and used row-local redraws for staged insert rows
  when the rendered grid shape stays stable.
- Made insert buffers show all fields by default and advertise the `C-c .`
  current-time shortcut in the header line.
- Clarified result footer cursor text by labeling column position as
  `Col current/total [column-name]`.
- Opened valid JSON object and array text cells in the JSON cell editor so
  JSON-like text fields get formatting and highlighting while editing.
- Made `C-c C-c` and `C-c C-k` in automatically opened JSON cell editors stage
  or cancel the whole cell edit instead of falling back to the compact parent
  edit buffer first.
- Kept result header horizontal scrolling aligned with the body when graphical
  pixel padding and icon sort indicators are active.
- Kept result headers aligned with rows when fallback sort indicators and
  short NULL columns are displayed together.
- Classified Oracle sources before row-identity metadata lookup, so dictionary
  views such as `ALL_TABLES` and `USER_TABLES` skip JDBC primary-key, column,
  index, and `ROWID` probes.  This prevents ORA-01445 and ORA-12592 while
  keeping `ROWID` fallback for confirmed base tables.  Schema-qualified JDBC
  queries retain the same schema for metadata and staged mutation targets.

## 0.2.2 - 2026-07-02

### Fixed

- Reduced graphical result-grid redraw work by caching plain cell pixel widths
  per character, avoiding display-space properties for ordinary padding,
  reusing rendered cells across redraws and same-shape page refreshes, skipping
  body scans when sorting the same rows, rendering rows in a single pass, and
  measuring only changed columns after manual column-width adjustments. Result
  buffers now also skip pixel layout entirely when the frame's font metrics
  already match Emacs logical cell widths.
- Fixed result rendering on Emacs builds where `string-pixel-width` accepts only
  one argument.

## 0.2.1 - 2026-07-02

### Fixed

- Aligned graphical result headers and body cells using measured pixel widths,
  including mixed ASCII/CJK font fallback configurations whose glyph widths do
  not follow a 1:2 ratio. Logical column sizing, terminal rendering, navigation,
  and existing column-width controls are unchanged. Result rendering now also
  uses the displayed result window's font metrics, keeps header-line horizontal
  scrolling aligned with pixel padding, and refreshes stale pixel layout after
  buffer-local face metric changes.

## 0.2.0 - 2026-07-01

This section summarizes the 0.2.0 release.

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

### Fixed

- Fixed copy transient loading with the Transient version bundled in Emacs 29.
  The copy refine toggle no longer depends on newer `:refresh-suffixes`
  support.

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
- Preserved manually adjusted result column widths when sort or paging reloads
  rows for the same column set.
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
