# 134 — Connection State Feeds UI Rendering

## Problem

`clutch-ui.el` rendered connection chrome by calling nine private connection
workflow functions.  Header evaluation reached upward for liveness, backend
identity, endpoint labels, namespace policy, and transaction state; result
footers also reached upward for transaction and spinner state.

That reverse edge kept UI inside a seven-module dependency cycle.  It also hid
several lifecycle bugs: result footers are cached strings, but transaction and
disconnect paths did not consistently rebuild them; failed query interruption
did not invalidate record/describe buffers; and a full result chrome refresh
could replace a DML outcome banner.  Query headers also need to detect a backend
that closes asynchronously, without waiting for a Clutch lifecycle command.

## Decision

The connection workflow now projects a small semantic plist into each attached
buffer.  It contains only rendering inputs: connected state, backend key and
label, connection label, namespace, schema state, and the transaction state
`auto`, `manual`, or `dirty`.  It contains no connection object, reconnect
params, callback, accessor, or pre-rendered text.

`clutch-ui.el` formats that data.  It no longer calls connection workflow
functions.  Schema contributes its raw state instead of a preformatted
header-line fragment.  The rapidly changing spinner frame remains a separate
buffer-local input so the 100 ms animation tick does not rebuild stable
connection state.  Query header evaluation rechecks only backend liveness and
combines it with the stable projection, preserving asynchronous loss detection
without rebuilding namespace/schema/transaction state on every redraw.

Existing lifecycle paths publish the snapshot directly:

- bind and rebind initialize it;
- schema and transaction transitions refresh affected attached buffers;
- disconnect and failed query-abandon paths invalidate every attached buffer;
- current and derived result buffers rebuild cached footers without touching
  their table or DML outcome header;
- spinner ticks update only execution presentation.

No generic event bus, provider callback, accessor facade, new module, or
render-state struct was introduced.  The connection workflow already owns all
of these transitions, so a direct plist projection is the smallest explicit
boundary.

## Why This Direction

Moving connection policy into UI would preserve the reverse dependency.
Letting UI pull values through newly named getters would only disguise it.
Having every presenter callback recalculate state would add indirection and
make the cached result footer lifecycle harder to reason about.

The snapshot makes ownership and refresh timing visible while preserving the
existing incremental redraw model.  A public disconnect test uses a real
in-memory SQLite connection and proves both the footer transition and result
header preservation.

## Consequences

- `clutch-ui` has no dependency on `clutch-connection`.
- Cross-module declarations fell from 150 to 140.
- The largest strongly connected component fell from seven modules to six;
  UI is no longer a member.
- Architecture checks now allow UI to depend only on backend and schema
  modules, preventing the reverse edge from returning.
- Result transaction indicators refresh when dirty state changes, and cached
  result footers show disconnect immediately without stale transaction text.
- Query headers detect asynchronous backend loss, query abandonment invalidates
  record/describe chrome, and DML outcome banners survive metadata refreshes.
- Header/footer tests consume semantic state instead of mocking connection
  implementation helpers.

## Deferred Boundary

UI still has two schema declarations for pending-insert placeholder metadata.
That query/scheduling responsibility ultimately belongs with the edit or
result workflow.  Moving it requires a clear placeholder refresh lifecycle;
adding another cache or callback solely to reduce two declarations would be
compensating glue, so it is deliberately left for a separate slice.
