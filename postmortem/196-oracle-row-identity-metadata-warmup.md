# 196 — Oracle Row Identity Metadata Warmup

## Problem

On Oracle/JDBC the first query of a session is noticeably slower than the second, even when the second runs different SQL against a different table. The delay is not in the query itself.

Row identity resolution runs synchronously before a SELECT reaches a new source table: `clutch--prepare-row-identity-query` calls `clutch-db-row-identity-candidates`, and the Oracle method first validates the table through `search-tables` and then asks for `get-primary-keys`. Both RPCs use bind parameters, so their first execution in a session pays a hard parse and a cold data dictionary while every later execution soft-parses. The agent's Oracle `search-tables` statement is a six-way `UNION ALL` that includes `all_synonyms`, and `DatabaseMetaData.getPrimaryKeys` expands to the driver's own constraint-catalog query. That first-use cost lands inside the user's first query, which is why a second, unrelated statement feels fast.

## Decision

Run both statements once in the background as soon as a connection is primed, using an object name that cannot match a row. The cost moves into the connection window, and the user's first query finds the statements already parsed.

Ownership follows postmortem 057: Clutch decides *when* metadata is requested, the agent decides *how* sessions are isolated. The warmup is therefore a Clutch-side scheduling decision expressed as `clutch-db-warm-row-identity-metadata`, a backend generic whose default does nothing. Only the JDBC method implements it, and it is gated on the same `clutch-jdbc--oracle-conn-p` predicate that selects the expensive path, so the warmup and the cost it removes cannot drift apart.

`clutch--prime-schema-cache` owns post-connect metadata priming and runs the warmup before scheduling schema refresh, because the first query waits on row identity, not on the schema cache.

## Why this is not idle-delayed

Postmortem 118 established that automatic schema refresh must wait a wall-clock delay so background metadata does not compete with the first foreground query. That rule does not transfer here, and the difference matters:

- Schema refresh is work the first query does not need. Row identity metadata is work the first query performs itself.
- Native backends share one protocol connection and the main thread. JDBC keeps an isolated metadata session per postmortem 057, and `clutch-jdbc--rpc-async` does not block Emacs, so the warmup cannot stall foreground SQL.
- If a query arrives while the warmup is in flight, it waits on the same statement it would otherwise have parsed itself. The warmup is never worse than doing nothing.

Deferring the warmup to idle time would reintroduce exactly the delay it removes.

## Rejected alternatives

### Cache row identity candidates per table

Considered as the primary fix. It removes repeat metadata round trips for tables already visited, which is worth doing on its own, but the first query on a fresh connection still finds the cache empty and still pays the hard parse. It does not address this problem.

### Warm the statements inside `clutch-jdbc-agent`

Equivalent in effect, but it makes a scheduling policy into agent behavior, contradicts the 057 ownership split, and requires a published jar and checksum pair for a change Clutch can make on its own.

### Widen the warmup to every metadata statement

Rejected as speculative. Only the two statements on the synchronous pre-query path have a demonstrated cost. The unique-index fallback runs only for tables without a primary key, and warming it would add connect-time work for every user to serve a conditional path.

## Consequence

Warmup is best-effort and silent: it matches no rows, and a failure is left for the real request to report with its own diagnostics. `clutch-debug-mode` records a `row-identity-warmup` submit event so the behavior is observable.

The `search-tables` warmup parses the statement the agent selects for the connection's current schema. A first query that is explicitly qualified with a *different* schema selects the other Oracle branch and still pays one parse; the `get-primary-keys` warmup covers every schema because the driver's internal query does not vary with it.

The warmup name must stay free of SQL `LIKE` wildcards. The agent appends `%` to the search prefix, so a `_` or `%` in it would turn the warmup into a real catalog scan; a regression test pins this.
