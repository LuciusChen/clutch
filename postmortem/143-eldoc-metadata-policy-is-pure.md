# 143 — Eldoc Metadata Policy Is Pure

## Problem

SQL Eldoc combined metadata policy with parsing, cache reads, synchronous and
asynchronous loading, comment warmup, and rendering.  Two tests therefore used
40 function replacements across 226 lines to exercise a small decision matrix.
One of those cases asserted that a busy connection queued column metadata from
the private renderer, even though the public Eldoc entry rejects busy
connections before calling it.  The test suite had turned an unreachable branch
into an apparent contract.

## Decision

Keep Eldoc in `clutch-sql.el`, but express its metadata choice as a pure plan.
The plan distinguishes table summaries, cached columns, synchronous column
loads, table-summary warmup, and skips.  The existing schema-string function is
the only effect boundary: it obtains statement context, executes load steps,
matches identifiers, and renders the result.

The busy-only private branch and its direct-call test were deleted.  The public
busy gate remains unchanged.  Table summaries still warm missing column names
and comments asynchronously; ordinary columns still use cached metadata first,
respect the backend's synchronous-completion policy, and allow a qualified table
to bypass the statement table-count limit.

No module, callback, registry, facade, or public API was added.

## Test Budget

The policy matrix is now table-driven and has no mocks.  One integration test
keeps the real SQL context parser and public Eldoc entry while replacing only
the metadata effect boundary.

The two tests shrank from 226 to 114 lines and from 40 function replacements to
11.  Production grew by 15 lines to expose the decision boundary, so the whole
change removes 97 lines.

## Consequences

- Short symbols and statements spanning too many tables still avoid metadata
  I/O, while qualified columns retain their focused lookup path.
- Table summaries, cached columns, synchronous misses, disabled synchronous
  completion, aliases, multiline statements, and error propagation remain
  covered without duplicating an effect harness for every case.
- A future Eldoc policy case extends the decision table rather than adding
  another nested branch plus mocks.
- Architecture metrics remain at 65 cross-module declarations, largest SCC two,
  and root state 14.
