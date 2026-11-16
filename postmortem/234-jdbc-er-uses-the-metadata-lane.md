# JDBC ER uses the existing metadata lane

The native ER asynchronous workflow already owns connection reservation, callback lifetime and cancellation sequencing. JDBC uses the same metadata request sequence and activity owner. The existing agent reader dispatches RPCs to its request pool and serializes metadata on a physical session separate from primary SQL. The transport adapter bridges the established success/error callbacks to the shared sequence. No Java execution layer, session, permanent cache or worker thread is added.

## Cancellation and compatibility

Agent `cancel` targets the tracked foreground Statement. Metadata includes driver DatabaseMetaData calls without an exposed Statement, so sending that operation for ER could cancel unrelated primary SQL. The activity runner accepts an optional operation-specific cancel function; ER uses the backend metadata interrupt contract, whose default delegates to normal interruption and whose JDBC implementation declines physical cancellation. The existing cancellation flag stops subsequent requests and rejects the result after the current RPC settles. The reservation remains held until then, preserving once-only completion and existing deadlines.

Incoming keys require the protocol addition in agent PR #8, version 0.2.28. The JDBC ER adapter checks the configured version and reports this requirement. The published 0.2.27 version/checksum remains intact until release verification. Local tests explicitly select a copied 0.2.28 build and isolated runtime.

## Shared column details

The existing asynchronous JDBC column detail reader returned only columns while its synchronous counterpart also read primary and foreign keys. Reusing that incomplete warm cache would lose key markers in ER. Both paths now share the three request definitions and decoder. Asynchronous details cost three RPCs instead of one; ER cold loads use these same three responses plus incoming keys without repeating them. Warm ER loads still issue one incoming-key request. Key names are retained when present, and unnamed rows remain independent instead of inventing a composite relation.

The existing column metadata regression was extended to both modes and failed before the fix. A focused incoming-key test covers scope, ordering, dispatch while primary is busy and logical cancellation. The unnamed-key regression also failed before its fix. Public live coverage exercises cold/warm loading, both key directions, composite grouping and cancellation without an agent cancel request.

## Namespace reads on DuckDB

The public live cancellation check caught three synchronous primary `execute` calls around an ER request. Their SQL was the existing DuckDB current-catalog/schema lookup, repeated by connection UI snapshots. DuckDB now returns its already remembered namespace and explicitly refreshes it after USE, SET or RESET, retaining the initial read when no namespace has been recorded. This reuses connection params, with no extra cache or freshness flag. The live check requires only the first metadata request after cancellation, protecting the shared UI/activity path from primary queries as well as unintended cancellation.

## Verification and local measurement

Emacs 32.0.50 passed the full non-live gate (734 main, 295 backend and 13 architecture tests), byte compilation, package-lint and checkdoc. The complete native/JDBC live run passed 267 applicable tests across 15 suites, including Oracle Free, SQL Server 2022, DuckDB, PostgreSQL 16 and MySQL 8.0; backend-specific capability skips remain explicit. ClickHouse used 24.8. Test containers were removed, and the volume names and count matched before and after (30). The isolated agent jar was built from PR #8 at d401e4e, SHA-256 4edca64e3d569041ab9e04e0afa15343afb768dfeb3f95da57e68a54829aa8ab.

A separate real DuckDB probe ran a long primary query concurrently with ER metadata, cancelled ER, observed its activity settle while primary remained busy, and then verified that the primary query completed successfully and SELECT 1 still worked. Local batch measurements used compiled code and a three-column parent/four-column child with single and composite keys. Five sequential runs per mode gave the following means. The synchronous comparison uses the same public ER command with asynchronous metadata declined, after connection context initialization; these are local fixture measurements without artificial network delay, not GUI frame-time or remote-server measurements.

| DuckDB cache | Sync command return | Async command return | Sync total | Async total |
| --- | ---: | ---: | ---: | ---: |
| Cold | 18.582 ms | 0.093 ms | 18.591 ms | 17.858 ms |
| Warm | 3.936 ms | 0.089 ms | 3.943 ms | 5.950 ms |

As on native connections, responsiveness improves while total loading can remain similar or increase with asynchronous dispatch. Cold standalone JDBC column details now use three RPCs for complete key information; warm ER remains one RPC. Remote latency and graphical frame timing remain unmeasured for JDBC.
