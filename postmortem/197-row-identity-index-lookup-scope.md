# 197 — Row Identity Must Not Enumerate a Schema

## Evidence

A `clutch-debug-mode` trace from a live Oracle connection (4461 indexes in the schema) showed the reported symptom: the first query of a session is slow, and a later query is fast even with different SQL against a different table.

Two events in that trace locate the cost. The executed SQL carried `ROWID AS "clutch__rid_0"`, so the table has no primary key and row identity fell through the whole candidate chain. Separately, the background object warmup for the same object category reported `Loaded 4461 indexes entries` and took about nine seconds.

`clutch-jdbc--unique-not-null-identities` sits in the middle of that chain and called `clutch-db-list-objects conn 'indexes`. For JDBC that generic issues a synchronous RPC with no table filter and consults no cache, so resolving identity for one table enumerated every index in the schema — inside the user's query, before it ran. A later query was fast because Oracle had the dictionary content cached by then, which is why the speedup did not depend on repeating the same SQL or table.

## Decision

Row identity asks the agent for one table's indexes. The `get-indexes` operation already accepts a `table` parameter, and the Oracle statement behind it already filters on `table_name`, so the schema-wide scan was never required; only the Elisp caller failed to narrow it. `clutch-jdbc--table-indexes` performs that scoped request, and `clutch-db-list-objects` remains what it is: the schema-wide object listing the browser and warmup want.

The rule this records: a synchronous pre-query metadata call must ask for what the query needs. Reaching for a broad listing because one already exists puts the whole schema on the critical path, and an uncached generic makes that cost recur.

## Rejected alternatives

### Read the object cache instead

`clutch-object.el` already caches warmed index entries, and the background warmup had loaded them before the traced query ran. Reusing them would have to cross from `clutch-db-jdbc` into `clutch-object`, which inverts the architecture and makes a backend depend on a UI-side cache. Scoping the request keeps ownership intact and is cheaper than a hit on the full cached list.

### Prefer ROWID and skip unique indexes on Oracle

Tempting, because Oracle always has a usable row locator and the trace ends at ROWID anyway. Rejected: it changes which identity a table gets, and a named unique key survives operations that invalidate a ROWID. The cost was in how the candidate was looked up, not in wanting the candidate.

### Warm the metadata statements at connect

This was tried first, in postmortem 196, and reverted. It was written from inference about Oracle hard parses without measuring the session, and it warmed `search-tables` and `get-primary-keys` while deliberately skipping the index path as a "minority case" — the exact path that carried the cost, for a table shape that is common in this schema. The lesson is procedural: a latency fix needs a measurement of the slow path before it is written, not a plausible mechanism.

## Consequence

Row identity on a table without a primary key now costs its own column details, one scoped index request, and one column request per unique index on that table, instead of a full-schema index enumeration.

Column details on this path remain an uncached per-table RPC. That is bounded by one table and is left alone here; deferred column metadata has its own lifecycle in postmortem 182.
