# 197 — Row Identity Metadata: Scope It, Trace It, Resolve It Once

## Evidence

A `clutch-debug-mode` trace from a live Oracle connection (4461 indexes in the schema) showed the reported symptom: the first query of a session is slow, and a later query is fast even with different SQL against a different table.

Two events in that trace locate the cost. The executed SQL carried `ROWID AS "clutch__rid_0"`, so the table has no primary key and row identity fell through the whole candidate chain. Separately, the background object warmup for the same object category reported `Loaded 4461 indexes entries` and took about nine seconds.

`clutch-jdbc--unique-not-null-identities` sits in the middle of that chain and called `clutch-db-list-objects conn 'indexes`. For JDBC that generic issues a synchronous RPC with no table filter and consults no cache, so resolving identity for one table enumerated every index in the schema — inside the user's query, before it ran.

Measured afterwards against a containerized Oracle with 3196 indexes, flushing the shared pool and buffer cache before each line:

| Step | Cold |
| --- | --- |
| Row identity, table with a primary key | 726 ms |
| Row identity, table without one | 1100–1400 ms |
| `column-details` | 792 ms |
| `get-primary-keys` | 314 ms |
| `search-tables` | 179 ms |
| `get-indexes`, one table | 42 ms |
| `get-indexes`, whole schema | 121 ms |
| The query itself | 39 ms |

The same resolution repeated on a warm instance takes 18 ms, so the large number is a cold data dictionary rather than any one call. Against the reporter's database the first resolution measured 487 ms and the query itself 946 ms — a full scan of 643,424 rows with no index on the filtered column, which is the database's cost and not Clutch's.

Two things made the metadata cost invisible and recurring. Resolution runs synchronously before execution but had no trace event, so it appeared only as a gap between events while the `execute` event's elapsed time covered the query alone. And nothing cached the result, so a table without a primary key walked the whole chain on every statement, not only the first.

## Decision

Row identity asks the agent for one table's indexes. The `get-indexes` operation already accepts a `table` parameter, and the Oracle statement behind it already filters on `table_name`, so the schema-wide scan was never required; only the Elisp caller failed to narrow it. `clutch-jdbc--table-indexes` performs that scoped request, and `clutch-db-list-objects` remains the schema-wide listing the browser and warmup want. The rule: a synchronous pre-query metadata call must ask for what the query needs.

Record a `row-identity` event with the table, the chosen candidate and its duration, next to the query it delays. The schema, foreign-key and object-warmup paths already have events; the one path that runs ahead of every execution had none.

Cache resolved candidates with the connection's other table metadata, keyed by catalog and schema as well as table (postmortem 127: identity belongs to one relation). Schema refresh, DDL, reconnect and schema switching already discard that store. A failed lookup is not cached; a transient metadata error must not leave a result permanently non-editable.

## Rejected alternatives

Reading `clutch-object.el`'s warmed index cache from the backend would invert the architecture and make a backend depend on a UI-side cache; the scoped request keeps ownership intact and is cheaper than a hit on the full list. Caching inside the backend generic would repeat the cache in every backend, none of which can see the schema-refresh lifecycle that says when to forget.

Preferring ROWID and skipping unique indexes on Oracle would change which identity a table gets, and a named unique key survives operations that invalidate a ROWID. The cost was in how the candidate was looked up, not in wanting it.

Warming the metadata statements at connect was tried first, in postmortem 196, and reverted. It was written from inference without measuring the session, and the measurements above show why it could not have worked: the large first-use cost is Oracle instance level, not per connection, so a new connection on a warm instance already resolves identity in tens of milliseconds. The lesson is procedural: a latency fix needs a measurement of the slow path before it is written.

## Consequence

The first statement against a relation resolves identity with one scoped index request instead of a schema enumeration; later statements against it reuse the answer. Verified against the reporter's database: 487 ms, then 2 ms. Execute-path tests isolate `clutch--table-metadata-cache`, which a resolved identity otherwise carries between them.

The first resolution on a cold, remote database remains the dominant cost before a query, and `column-details` is its largest single call. Deferred column metadata has its own lifecycle in postmortem 182.
