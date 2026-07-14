# 152 — Dependencies Should Not Pretend to Be Lazy

## Problem

After redundant declarations were removed, 43 cross-module declarations
remained.  Some marked real lazy or cyclic boundaries, but 26 hid ordinary
same-layer dependencies.  Connection always rendered through UI, object
commands depended throughout on connection and UI, and edit, document, query,
and UI workflows directly called their declared owners during normal use.

Those files were not independently usable without the composition root's load
order.  Calling the edges lazy saved no work in a normal package load and made
the apparent dependency graph less truthful than the runtime graph.

## Decision

Use mandatory top-level requires for owners that a module needs for its normal
workflow: connection requires UI; document requires connection; edit requires
connection; object requires connection and UI; query requires schema; and UI
requires schema.  Remove the declarations covered by those contracts.

Keep declarations where deferral is real: query and result must not eagerly
close their execution/presentation cycle, document and query expose optional
object/result commands, adapters load optional implementations conditionally,
and Redis must not make a lower-level adapter eagerly load the query workflow.

## Alternatives Rejected

Keeping the declarations would preserve a smaller nominal load surface only
for files that could not actually complete their normal work in isolation.
Replacing the two remaining strongly connected pairs with registries or
callback plumbing would trade direct, understandable workflow relationships
for glue without creating a new owner.

## Consequences

- Cross-module declarations fall from 43 to 17.
- The largest strongly connected component remains two: query/result and
  backend/JDBC each retain one genuine deferred boundary.
- Diagnostics remains a backend-only leaf and does not acquire a reverse
  dependency on connection or the composition root.
- Future declarations must identify an actual lazy, optional, or cyclic load
  boundary rather than compensate for composition-root ordering.
