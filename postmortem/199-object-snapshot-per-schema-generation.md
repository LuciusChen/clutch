# 199 — The Schema Listing Stays Off Describe and the Warmup

## Evidence

`clutch-describe-dwim` on an Oracle table stayed slow after the row-identity and agent work of postmortems 197 and 198. A read-only probe on the reporting database (32,780 tables, 4,461 indexes) timed every call the describe view makes for one table. The metadata it needs is cheap there: `search-tables` 60 ms, `get-primary-keys` 17 ms, `get-foreign-keys` 17 ms, `get-columns` about 220 ms. The describe sections still took 3.1–3.3 s warm, and 17 s the first time.

The time was in `clutch--browseable-object-entries`: 1.3–1.6 s per call on that schema, and never cached. It asks the backend for `clutch-db-browseable-object-entries`, which on every JDBC product is a fresh `get-tables` listing (33 fetch round trips here) and on the default backends is a table listing plus an empty-prefix search. Every object snapshot merges it in, so the describe view paid it twice, once to find the table's indexes and once for its triggers; each background warmup category paid it again inside its callback, which is where the 17 s first describe came from, since those synchronous listings ran while the describe's own requests queued behind them. Caching the snapshot per schema generation fixed the steady state but not the moment after connecting: the snapshot was not cached yet, and the schema refresh's install dropped it once more, so the first describes of a session still paid the listing.

## Decision

Describe and the warmup do not need the snapshot. The related-index and related-trigger lookups read only the warmed categories, which is all they ever matched (a table entry is never an index or a trigger); when a category is not warmed yet they return nothing and schedule the warmup, as the partial snapshot already did. The warmup's category store no longer merges the table snapshot into the object cache, which no consumer read from there. So neither path lists the schema, cold or warm, and a describe right after connecting costs its own metadata only.

The snapshot still exists for the picker and browser fallbacks that show tables. `clutch--browseable-object-entries` lists it once per schema cache generation, primes table comments from it (the category store used to do that on every step), and drops it where the object cache is already dropped, when the schema cache reports `invalidated` (a refresh installing new names, a reconnect, a schema switch, cleared metadata). An explicit object refresh lists again and replaces the cached snapshot, so the browser's refresh still reaches the database.

Nothing changes in how any backend lists objects.

## Other backends

The structure is the same everywhere; only the magnitude differs. Every JDBC product lists the whole schema per snapshot, so SQL Server, DB2, Snowflake, DuckDB and ClickHouse pay one listing per describe and per warmup step in proportion to their table count and latency. MySQL, PostgreSQL and SQLite run two statements per snapshot through the default method. None of them showed seconds, because their schemas in use are small and local, but the fix is in the shared object layer and applies to all of them without touching a backend.

## Rejected alternatives

### Cache the snapshot and leave the callers alone

This was the first version: one listing per schema generation, every caller unchanged. It removed the steady-state cost but left one synchronous listing in the connect window, charged to whichever came first, the user's describe or a warmup callback running inside it. Taking the snapshot out of the two paths that never needed it removes that listing instead of moving it.

### Derive the snapshot from the schema cache

The schema refresh already lists the same tables, but it keeps only names; the snapshot needs types, schemas and comments, so a second listing per generation is the cost of not widening the schema cache. Two listings per generation is where this lands; the previous state was two per describe.

### Cache inside each backend

Would need one cache per backend implementation, each with its own invalidation, for a lifecycle the object layer already owns.

## Consequence

On the reporting database a table describe pays no schema listing at any point in the session, only its own metadata (about 0.35 s there); its Indexes and Triggers sections appear once the warmup has loaded those categories, as before. The warmup no longer lists the schema at all; the object picker's full-listing fallback lists once per schema generation. The snapshot is as fresh as the schema cache: an object created outside clutch appears after the next schema refresh, which is already the rule for completion.
