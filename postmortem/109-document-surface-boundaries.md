# Document Surface Boundaries

## Context

MongoDB support introduced a second data model next to relational SQL. The first implementation reused table-oriented Clutch workflows and added MongoDB-specific branches in the object UI where the UI needed native collection behavior.

That worked for a single document backend, but it made the shared object layer know too much about MongoDB. Adding another document backend would have required copying those branches or teaching the UI another backend name.

## Decision

Keep backend-specific query syntax in adapters. The object UI asks the backend for an object browse query through `clutch-db-object-browse-query`; when no backend-specific query exists, relational connections keep the existing SQL `SELECT *` behavior.

Object definition is also backend-owned. The old table-oriented `clutch-db-show-create-table` and `clutch-db-show-create-object` split was removed in favor of `clutch-db-object-definition`, which receives the full object entry. SQL adapters can still return DDL/source text, while MongoDB can return collection or index metadata JSON without pretending that collections are tables.

Use the registry `:data-model` as the shared test for native document surfaces. The object UI may use document data-model semantics for generic behavior such as JSON metadata display. Query consoles use the backend query mode, but metadata buffers use JSON display mode. MongoDB-only actions remain explicitly MongoDB-only because helpers such as `listIndexes()` and sample `explain()` are not a portable document database contract.

## Consequences

Future document backends can provide their own browse query without changing `clutch-object.el`. If a native document backend forgets to implement object browsing, the UI errors instead of silently generating SQL for a non-SQL surface.

Future non-relational backends can provide object definitions in their own native representation. The shared object UI no longer needs to know whether a backend has tables, collections, buckets, indexes, or some other object shape before asking for the definition.

This does not yet create a full document database capability registry. That should wait until a second document backend proves which actions are actually shared across document databases.
