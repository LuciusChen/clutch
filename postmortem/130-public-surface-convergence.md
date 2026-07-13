# 130 — Surface and State Convergence

## Context

Several public entry points and configuration spellings overlapped after earlier
workflow additions:

- `clutch-execute-dwim` already selected the active region, a semicolon-scoped
  statement, or the query at point.
- `clutch-execute-query-at-point` and
  `clutch-execute-statement-at-point` exposed narrower variants of that same
  default execution model.
- `clutch-describe-table`, `clutch-describe-table-at-point`, and
  `clutch-browse-table` exposed table-specific variants of the object workflow.
- TRAMP connection configuration accepted both `:tramp` and
  `:tramp-default-directory`, while internal connection state already used
  `:tramp-default-directory`.

These overlaps made the surface larger while exposing the point-boundary
heuristic as separate commands.  Exact execution bounds remain available
through an active region.

The same duplication existed below the command surface:

- insert forms kept rendered text, seed fields, all-column lists, and schema
  detail maps as independently mutable representations of one form
- result buffers cached primary-key indices already present in row identity
- object discovery stored both flat entries and a derived by-type index
- table column and foreign-key metadata used five parallel connection/table
  hash tables for values and statuses
- object actions had registry entries, Transient wrappers, inapt wrappers, and
  a second label table describing the same commands
- the public insert major-mode wrapper could create a form without the result
  and schema state required to make it usable
- reconnect paths swallowed arbitrary errors or silently built a connection a
  second time

## Decision

- Keep `clutch-execute-dwim`, `clutch-execute-region`, and
  `clutch-execute-buffer` as the public execution commands.
- Remove the explicit query-at-point and statement-at-point commands from the
  public command set and dispatch menus.
- Remove table-specific object wrapper commands; use the generic object
  describe/action workflow.
- Keep the insert form mode internal and enter it through the result workflow
  that supplies its canonical field state.
- Keep the lower-level point-boundary helpers private, because DWIM execution
  and preview still need them.
- Use `:tramp-default-directory` as the only TRAMP connection parameter.
  `:tramp` now fails fast instead of being canonicalized.
- Make insert field plists the sole form state.  Rendered text is a view over
  that state and is never reparsed as a fallback.
- Derive primary-key indices and object type subsets from their canonical
  sources instead of caching copies.
- Store column, foreign-key, and comment values/statuses in one concrete
  table-metadata cache.  Comments use schema-qualified table keys; no generic
  cache framework is introduced.
- Bind object menu and Embark actions directly to public action commands and
  derive labels and availability from the action registry.
- Let reconnect/build errors propagate through the existing command boundary;
  do not retry connection construction implicitly.

## Why

The DWIM command is the user-facing execution model.  Keeping separate public
commands for its internal branches meant users had to choose between overlapping
actions whose usual behavior was already represented by one default.  The
explicit commands could force blank-line or semicolon-only point boundaries;
after their removal, users select a region when the DWIM boundary is not the
intended payload.  This deliberately trades branch-specific point commands for
one visible execution model and one standard exact-boundary mechanism.

The object workflow has the same shape: object resolution and action dispatch
already know whether an entry is browseable or describeable.  Table-only wrappers
made the command list longer while bypassing the action vocabulary users see in
the object menu.

For TRAMP, choosing the existing internal spelling avoids adding another
translation stage.  A stale `:tramp` key should not be ignored, because that
would silently connect without the intended remote origin.

For internal state, the reduction rule is narrower than “put everything in one
struct.”  State is consolidated only when two stores represent the same fact or
share the same connection/table lifetime.  Pagination offsets, local/server
sort state, render caches, schema-qualified comments, and async queue/active
state remain distinct because they have different semantics or invalidation
rules.  A general state or cache abstraction would add accessors and lifecycle
machinery without removing those distinctions.

Tests now open insert forms through the production entry point and mutate real
field regions.  Tests that created arbitrary form text and relied on the mode to
reverse-engineer state were removed or rewritten; restoring that parser would
reintroduce a second input model solely for tests.

## Consequence

This is a breaking surface cleanup for users who call the removed commands or
configure saved connections with `:tramp`.  The migration is direct:

- call `clutch-execute-dwim`; select a region first when exact boundaries matter
- call `clutch-describe-dwim` or `clutch-act-dwim`
- call `clutch-result-insert-row` from a result buffer
- use `:tramp-default-directory`

Internally, errors from reconnect and backend capability queries now reach the
normal outer error boundary instead of being converted into false capability
answers or silent retries.
