# 153 — Diagnostics Stores Provenance, Not Callbacks

## Problem

Diagnostics needed a source buffer and connection label when replaying an old
problem.  The extraction solved that by making connection register two global
callbacks at load time.  Diagnostics then called those accessors in reverse
and silently swallowed any error they raised.

That registry made two owners depend on initialization order, kept shared
function slots solely for indirection, and recovered information after the
fact even though the source buffer was already known when a problem was
recorded.

## Decision

Store connection-scoped problems as provenance envelopes containing the source
buffer and the problem.  Historical replay reads that data directly.  A
recoverable metadata warning belongs to the buffer in which its effect runs,
so it records `current-buffer` rather than searching every Emacs buffer for a
connection attachment.

Render the debug connection label from the backend contract's key, display
name, user, host, port, and database.  These values describe the effective
endpoint and do not expose connection plists, URLs, passwords, or opaque
objects.  Backend contract failures propagate instead of being hidden behind
an internal catch.

## Consequences

- Diagnostics no longer owns callback slots or a registration API, and
  connection has no load-time diagnostics registration side effect.
- Problem replay preserves its original source even when invoked from another
  buffer.
- SSH and TRAMP labels show the effective forwarded endpoint; the redacted
  connection context still carries the original transport details needed for
  troubleshooting.
- Diagnostics remains a backend-only leaf and does not depend on connection or
  the composition root.
