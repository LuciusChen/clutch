# 133 — Connection Identity Owns Metadata Lifetime

## Problem

Schema, table metadata, async tickets, and object warmup state were keyed by
`clutch--connection-key`.  That value is a display label such as
`user@host:port/database`, not the identity of a live database connection.

Two independent connections with the same label could therefore share cache
entries and refresh each other's result buffers.  Reconnect and schema-switch
paths compensated by carrying old display keys and clearing multiple cache
namespaces.  `clutch-schema.el` also reached upward to refresh connection UI and
directly invalidate object-owned cache and warmup state.

The same identity coupling appeared in tests: metadata tests repeatedly replaced private
connection-label and UI functions merely to establish cache identity.

## Decision

Connection-scoped metadata state is keyed directly by the connection object,
using `eq` hash tables.  Display labels remain presentation only.

- Schema cache, status, table metadata, async queue/active state, help docs,
  install timers, and freshness tickets use connection identity.
- Object discovery cache, warmup timers, and generations use the same identity.
  Generation keys are weak so freshness tracking cannot extend a retired
  connection's lifetime while outstanding callbacks still keep it reachable.
- Reconnect clears the old and new connection objects explicitly.  Switching a
  namespace on one connection clears that connection once; no display-key
  compatibility path remains.
- Buffer refreshes compare attached connection objects with `eq`, preventing an
  update for one session from touching another session with the same label.

No generic cache framework or session accessor layer was added.  The existing
stores retain their distinct invalidation semantics.

## Dependency Direction

`clutch-schema.el` is now a lower-layer metadata producer.  It requires only the
backend contract and diagnostics, and publishes two narrow lifecycle hooks:

- schema cache state changed (`invalidated` or `ready`)
- metadata state changed for a connection

The object workflow owns its response to schema invalidation and readiness.
The connection workflow owns attached-buffer UI refresh.  Interactive schema
refresh and reconnect orchestration live in the connection workflow; schema
services receive an explicit connection object.

`clutch-sql.el` also reads liveness through `clutch-db-live-p` instead of
requiring the connection workflow for a one-line wrapper.

Architecture checks make the direction mandatory: schema may depend only on
backend and diagnostics, while SQL context may depend only on backend and
schema.  The refactor reduced cross-module declarations from 168 to 150 and the
largest strongly connected component from 9 modules to 7.

## Consequences

- Same-label connections have isolated metadata and UI lifecycles.
- Stale async work remains scoped to the connection object that created it.
- Warmup freshness bookkeeping no longer prolongs retired connection lifetimes.
- Display-key plumbing and direct schema-to-object cache manipulation are gone.
- Tests can exercise connection identity directly instead of mocking a private
  presentation helper.

This change deliberately does not consolidate all metadata stores into one
state object.  Queue, active request, status, schema contents, and table
metadata have different update and invalidation rules; merging them would add
accessor machinery without removing those distinctions.
