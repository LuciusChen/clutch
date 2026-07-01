# MongoDB Collection Stats Action

## What Changed

Clutch added a MongoDB collection `Show stats` object action.  The action displays
collection-level storage metadata such as `count`, `avgObjSize`, `storageSize`,
`totalIndexSize`, `totalSize`, and per-index `indexSizes`.

## Why It Is an Object Action

Collection stats describe a persistent MongoDB collection, not one query result.
SQL result footers in Clutch describe the current result buffer: fetched rows,
pagination, elapsed time, and staged mutation state.  Showing MongoDB collection
stats there would make every ordinary document query imply a separate metadata
request and would blur the difference between result state and object metadata.

The object action model keeps the UX consistent with existing collection actions
such as index listing, validation metadata, and sampled explain, while preserving
the document-specific meaning of the stats.

## Protocol Boundary

The adapter uses the public `mongodb-aggregate` API with a `$collStats`
aggregation stage.  It does not call `mongodb--*`, does not add BSON/protocol
logic to Clutch, and does not expand the basic MongoDB helper parser with a
deprecated `collStats` shell command.

## Deferred Work

The action is read-only and intentionally does not add performance dashboards,
index recommendations, historical metrics, or mutation controls.  Those would be
separate document-database workflows rather than a small collection metadata
surface.
