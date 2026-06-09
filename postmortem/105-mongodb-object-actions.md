# 105 - MongoDB Object Actions

## What changed

MongoDB collections now participate more fully in Clutch's existing object
workflow:

- collection Describe shows sampled fields and collection indexes
- Show definition displays `getCollectionInfos({name: ...})` JSON in
  `clutch-mongodb-mode`
- collection object actions can execute `listIndexes()`
- collection object actions can execute a sample
  `find({}).limit(1).explain("executionStats")`
- native MongoDB indexes map into generic `INDEX` object metadata

## Why

MongoDB GUI clients commonly expose documents, schema samples, indexes, and
explain plans as first-class collection workflows.  Clutch already has a
unified object action model for SQL objects, so the smallest coherent change is
to extend that model for MongoDB collections instead of creating a separate
MongoDB-only transient.

This keeps the user-facing model consistent:

- `C-c C-o` remains the place for object actions
- `C-c C-d` remains the place for object description
- collection browsing remains the default table-like action
- backend-specific actions are still guarded by object type and connection
  checks

## Boundary

The Clutch side remains UI and adapter code only.  It calls public
`mongodb-` APIs through `clutch-mongodb.el` and does not implement BSON,
wire protocol, authentication, cursors, server selection, pooling, or private
`mongodb--*` behavior.  Those responsibilities stay in the external
`mongodb.el` package.

The new actions intentionally generate supported MongoDB helper syntax.  They
do not attempt to implement a visual aggregation pipeline builder, a JavaScript
runtime, or a full Compass-style explain-plan renderer.

## Deferred work

If Clutch later supports Redis or another non-relational backend, object
actions should continue to be capability-driven rather than MongoDB-driven.
Redis should not reuse the MongoDB collection/index vocabulary unless its own
backend contract can honestly expose equivalent capabilities.
