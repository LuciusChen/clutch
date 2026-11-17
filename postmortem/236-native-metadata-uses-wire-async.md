# Native metadata uses asynchronous wire requests

## Evidence

The PG and MySQL native adapters scheduled synchronous catalog readers in idle timers. Deferring the start let the initial schema paint, but the timer body still waited for the network. A disposable OrbStack PostgreSQL 16/MySQL 8.0 probe used a local proxy that delayed each server response by 100 ms. Calling the real public metadata entry points and timing their scheduled bodies measured PG object warmup at 223 ms, column details at 337 ms, and names, comments, keys and schema reads around 106–117 ms. MySQL details took 338 ms and its other idle readers around 107–109 ms. The timing baseline already included the initial local MySQL object-warmup fix, which is included in this PR. The proxy reproduces a remote wait under controlled conditions.

## Implementation

Reuse native wire senders, the existing metadata request-series runner, existing SQL builders and row decoders, and existing cache-generation guards. Schema refresh preserves its initial idle delay. PG object rows include the schema in their result, so a cold namespace cache does not introduce a synchronous preflight lookup. Synchronous and asynchronous object reads share their SQL and decoder, retaining routine OID identity and multi-event trigger grouping.

Column-detail preheat and ER share one three-request column/key reader per adapter. ER then obtains incoming keys using its existing cancellation continuation. A background flag at the native sender keeps metadata out of a connection reserved by a foreground batch; ER's own activity can still send its metadata. Existing connection ownership, request-series scheduling and metadata caches retain their roles.

Real workflow tests caught foreground editing, submission and schema switching attempting to use a connection still reading metadata. Connection admission, the synchronous SQL runner and schema switching reuse the existing bounded background-request wait. The pure foreground refusal guard remains unchanged, so an asynchronous ER command can return immediately while an older request finishes. Direct server-probe fixtures wait before bypassing command admission. Transaction admission now relies on the shared connection check rather than a separate wait of its own.

Native asynchronous protocol calls do not apply the synchronous read-idle timeout. These metadata reads inherit that contract, including server-side statement limits. A command waiting for the shared session has the existing ten-second interruptible wait; quitting it leaves the asynchronous reader to finish. This is a change from the old synchronous reader's client read-idle deadline and is documented in the guide. XTDB retains its dialect-specific idle fallback, including unqualified field completion. SQLite remains local and synchronous. MongoDB's field-sampling adapter has the same idle/synchronous boundary, but the installed mongodb.el exposes no asynchronous command API; converting its wire implementation remains separate work.

## Verification

The regression for native public metadata entry points fails on the preceding code at the synchronous idle fallback. It checks dispatch, deferred replies and foreground reservation. Existing idle-delay and XTDB tests were adapted to their actual contracts rather than a timer object's identity. The existing ER live fixture also compares all native asynchronous metadata readers and object categories to their synchronous counterparts.

An independent before/after test compared actual catalog results against the preceding implementation on separate fixture schemas. All 20 PG/MySQL combinations matched, including composite foreign keys, generated fields, defaults, Unicode comments, a sequence, overloaded PG functions, routine identities and a multi-event trigger. Disconnecting a pending request through the delayed connection produced exactly one error callback on each backend and closed the session.

The complete native/JDBC live runner passed 267 applicable tests in 15 suites, including PG, MySQL, XTDB, MongoDB, Redis, Oracle, SQL Server, DuckDB and ClickHouse 24.8. Backend-specific skips are explicit. The final non-live gate passed 736 main, 297 backend and 13 architecture tests on local Emacs 32.0.50, with byte compilation, package-lint and checkdoc clean. All follow-up live tests used disposable OrbStack databases.

## Compiled-code performance

Five runs per backend and operation used compiled adapters with the same 100 ms response-delay proxy. PG idle-body occupancy averaged 221 ms for objects, 338 ms for column details and 109–112 ms for single-query readers. MySQL column details averaged 328 ms and its remaining idle readers 110–117 ms. Converted readers issued zero synchronous query calls. Their entry points averaged 0.20–0.52 ms, and the schema idle callback averaged 0.29 ms. Detailed-column completion remained around 335–349 ms including three real network replies. PG object completion fell from 228 ms to 121 ms because its schema now arrives with the rows. These are controlled local network-delay measurements.

A foreground-reservation mutation bypassing the sender guard fails the installed-path regression. Closing an in-flight metadata connection through the delayed proxy yields one error callback on each native SQL backend. The SQLite local API was also exercised separately on a disposable file with 200 tables and a 500-column table.

SQLite's ten-run mean was 0.06 ms for names of 200 tables, 0.25 ms for 500 column names and 2.12 ms for detailed columns, with an 11.50 ms maximum including allocation/collection. Its local synchronous path remains appropriate for this fixture. All disposable test containers and the response-delay proxy were removed, and Docker volume inventory was identical at 30 before and after. The tested adapters and connection admission changes were compiled and hot-loaded into the local preview.

PR preparation added a missing old-client guard to MySQL object warmup. Its existing regression fails with a missing `mysql-query-async` before that guard and passes afterward. The XTDB idle test now has one connection binding and one case table, eliminating the obsolete second-backend branch. This PR is stacked on #225, with the local ER presentation experiment excluded from its diff.
