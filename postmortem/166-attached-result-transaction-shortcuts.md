# 166 — Transaction shortcuts in attached result views

## Context

Clutch already attached the live connection, reconnect parameters, and SQL product to Result Browser and Record buffers. The transaction commands therefore worked there through `M-x`, but their keys existed only in Query Console and REPL maps. This made the normal Oracle workflow unnecessarily discontinuous: `C-c C-c` executed a staged mutation batch in the result, then the user had to switch buffers before `C-c C-m` could commit the now-dirty server transaction.

The two commands must remain distinct. Result `C-c C-c` submits locally staged INSERT/UPDATE/DELETE statements; on a manual-commit connection those statements are still reversible. `C-c C-m` commits that server transaction and must not execute local staged state.

## Decision

Keep one shared transaction-key vocabulary:

- `C-c C-m` calls `clutch-commit`.
- `C-c C-u` calls `clutch-rollback`.
- `C-c C-a` calls `clutch-toggle-auto-commit`.

Query Console and REPL maps install that vocabulary directly. Attached Result Browser and Record views receive it through a private minor-mode map synchronized when their connection context is bound. The minor mode is enabled only when the live backend reports manual-commit support.

## Why connection binding owns activation

The bug was command routing, not transaction execution. Result and Record already carry the exact connection object, and the transaction commands already resolve that buffer-local object correctly. Adding forwarding wrappers or teaching commands to search for a parent Query Console would duplicate connection resolution and fail across reconnects.

Connection binding is the lifecycle point that already installs and replaces attached connection identity. Synchronizing the minor map there makes a reconnect update both the command target and its availability together. Explicit disconnect and owner-buffer invalidation also synchronize after clearing the connection, so detached Result and Record buffers cannot retain live-looking transaction keys.

## Capability boundary

The transaction map is not part of the shared result major-mode map. Native MongoDB and Redis results reuse the grid but do not expose SQL transaction controls; SQLite also reports the controls unsupported. Capability-gated activation preserves those boundaries while making every supported SQL result representation consistent.

## Performance note

Executing a staged batch still refreshes through the normal result rerun contract so filters, triggers, generated values, and row membership reflect database truth. Expensive original queries can therefore dominate the perceived submit time. Skipping that refresh by default would trade latency for stale or incorrect rows and is not part of this key-routing fix.
