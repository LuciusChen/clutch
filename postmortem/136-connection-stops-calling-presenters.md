# 136 — Connection State Does Not Call Workflow Presenters

## Problem

The connection lifecycle still called two higher-level workflow functions.  It
asked query to rename console buffers and object to rebuild describe buffers
after metadata changed.  Those two presentation calls kept connection, query,
result, edit, and object in one strongly connected component even though
connection already projected semantic render state for UI consumers.

The object call also depended on `clutch-describe-mode`.  Native MongoDB
describe buffers use JSON mode, so they installed the same refresh command but
were skipped by the connection-owned refresh branch.

## Decision

Connection remains the owner of connection and metadata lifecycle transitions,
but it no longer calls query or object presenters.

- UI renders a console buffer name from a plain name, schema state, and table
  count.  It does not inspect a connection.
- Connection reads the schema state, calls that pure renderer, and performs the
  buffer rename.  Query creates a temporary buffer and asks connection to apply
  the canonical name after binding its context.
- Describe buffers use the standard buffer revert protocol they already
  advertised.  Connection invokes `revert-buffer` with `IGNORE-AUTO` and
  `NOCONFIRM`; object redraws from current metadata without invalidating caches
  or starting another schema refresh in that automatic path.
- Result and query branches remain ahead of the generic revert branch because
  their cached chrome has narrower incremental refresh rules.

No callback registry, new hook, accessor facade, integration module, or
forwarding command was added.  The existing connection-to-UI projection
boundary gained one pure formatter, and object uses an Emacs protocol instead
of a package-private reverse call.

## Test Budget

The connection identity test now drives the real console rename and standard
revert paths instead of mocking the removed query helper.  Manual and automatic
describe refresh share one test that distinguishes invalidation from redraw.

Query-console tests retain references to the buffers they actually open and
clean those buffers directly.  They no longer reconstruct production buffer
names through a private helper.  The main test count remains 487.

## Consequences

- Connection depends only on backend, diagnostics, schema, and UI modules; an
  architecture allowlist prevents presenter dependencies from returning.
- The largest strongly connected component fell from five modules to three.
  The remaining component is query/result/edit.
- Cross-module declarations fell from 137 to 136, with no stale declarations.
- SQL and native document describe buffers now follow the same metadata redraw
  protocol without recursive schema refresh.

## Deferred Boundary

Query, result, and edit still form a real execution/mutation cycle.  Several
connection-owned configuration and buffer-context variables also remain in the
composition root.  Those concerns need separate ownership and behavior
invariants; neither requires restoring a connection-to-presenter dependency.
