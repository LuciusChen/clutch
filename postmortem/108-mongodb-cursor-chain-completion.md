# MongoDB Cursor Chain Completion

_Updated by 111: collection profiles now analyze nested document paths, resolving the nested-field limitation recorded here._

Clutch now completes MongoDB cursor helper chains after `find(...)` and
`aggregate(...)` in native MongoDB query buffers.

## Why

MongoDB GUI clients make query controls first-class.  Compass exposes filter,
projection, sort, skip, limit, max-time, explain, schema, indexes, validation,
and aggregation workflows around a collection.  Clutch already exposes the
collection metadata side through object actions: describe, definition, indexes,
stats, validation, and sample explain.

The remaining high-value gap was discoverability for query controls.  The native
MongoDB backend already supports helpers such as:

- `find(filter, projection).sort(...).skip(...).limit(...)`
- `find(...).explain(...)`
- `aggregate(...).allowDiskUse(...).maxTimeMS(...).explain(...)`

Before this change, completion offered collection methods after `db.users.`, but
not cursor methods after `db.users.find({}).`.

## Decision

Use ordinary Emacs completion instead of a MongoDB-only transient or query
builder.  This keeps MongoDB consistent with SQL query buffers, where users
type the real query language and rely on completion for keywords, identifiers,
and functions.

Completion is capability-shaped:

- `find(...)` offers `sort()`, `skip()`, `limit()`, `maxTimeMS()`,
  `batchSize()`, `allowDiskUse()`, `comment()`, and `explain()`.
- `aggregate(...)` offers only chain helpers that Clutch maps to aggregate
  command options or explain.
- `explain()` is treated as terminal, so Clutch does not suggest another cursor
  chain after it.

This is deliberately not a Compass-style visual query builder and not a full
`mongosh` parser.  The backend helper parser remains the source of truth for
what can execute.

## Follow-ups

Schema analysis remains the larger missing document-database workflow.  Clutch
currently samples top-level fields for metadata; a richer nested field analyzer
would need a separate design because it can become expensive and would likely
need result-buffer UI rather than only completion.
