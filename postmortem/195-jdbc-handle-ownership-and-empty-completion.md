# 195 — JDBC handle ownership and empty completion

## Evidence

A real DuckDB probe retained a handle across a JVM restart. Both processes
assigned connection id 1: the old handle could query the new database, and
disconnecting it removed the new connection. Checking liveness only in the UI
did not protect direct backend calls; recovering an owner from the numeric RPC
parameter discarded the original handle's process identity.

Installed MongoDB completion also retried successful empty column metadata on
each invocation. A nil schema value meant both unloaded and loaded-empty.
Collection-like text inside a string or comment could redirect field completion
to the wrong collection because the resolver searched raw text.

## Decisions

Pass the original connection into synchronous RPCs and validate process plus
registered owner at sync, query and async request boundaries. Scoped metadata
copies keep their schema parameters but use the canonical owner for lifecycle
and diagnostics. Old disconnects clean local state without touching a reused id.
Retiring an agent clears its connection registry. No wire-format or id-generation
change is required, and the fix works with the pinned 0.2.21 agent.

Retain a ready state only for column names, including nil success, until schema
refresh. Foreign-key and other metadata states keep their existing semantics.
Search backward for the nearest collection expression in code, using the buffer's
syntax state to skip strings/comments. Do not add a JavaScript parser or a second
completion cache.

## Verification and scope

New tests fail before the fixes and pass afterward through public JDBC operations
and installed completion. Controls cover current handles, scoped metadata,
nonempty columns, refresh, synchronous fallback and both comment forms. Live
checks include JVM restart with DuckDB and the native/JDBC container matrix.

Keep the alternate direct table payload: the confirmed maintenance issue was a
reversed current/legacy comment and duplicate cursor tests, not proof that every
supported alternate payload is obsolete. The separate Java bounded-cancellation experiment was withdrawn before commit or release: a fixed one-second deadline and admission through the shared execution pool could retire recoverable connections. Blocking cancellation remains known debt; this Clutch fix does not require an agent release or checksum change.
