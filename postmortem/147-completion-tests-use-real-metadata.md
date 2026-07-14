# 147 — Completion Tests Use Real Metadata

## Problem

Eleven SQL completion tests repeated 45 function replacements across 339
lines.  Most rebuilt the same fake schema, backend, and CAPF environment, so
small implementation changes required editing many harnesses.  Two mocks
guarded an asynchronous path that the public completion entry could not reach,
and several parser replacements prevented the tests from exercising the
dispatch behavior they claimed to protect.

Adding a completion-context object would have moved this test complexity into
production.  Completion has one real caller and already returns the standard
Emacs CAPF shape, so another model would not establish a useful ownership
boundary.

## Decision

Keep the production completion path unchanged.  Replace the repeated harnesses
with three workflow tests:

- an installed CAPF running against fresh in-memory SQLite metadata;
- a compact effect matrix for ready, missing, stale, and direct-column sources;
- the Oracle i18n fail-soft boundary.

Only the first test depends on SQLite availability.  The effect matrix and
error-boundary test use connection structs without external resources, so the
core routing contract cannot disappear behind a conditional skip.  Exact
candidate lists are asserted only where exclusivity or table priority is the
contract; additive keyword and identifier cases assert required and forbidden
members instead of incidental ordering.

## Test Budget

The completion slice falls from 11 tests to three, from 339 lines to 170,
and from 45 function replacements to six.  The replacement still drives
`clutch-mode`, its installed completion functions, Emacs completion filtering,
real SQL parsing, cache hits and misses, qualified columns, case conversion,
backend fallback, and warn-once behavior.

## Consequences

- Completion behavior is tested through the public dispatcher instead of a
  separately invented context layer.
- Short prefixes prove that column metadata remains unloaded, while a real
  synchronous cache miss proves that metadata is installed.
- Backend-specific sources remain isolated by a narrow matrix without making
  SQLite a requirement for core completion coverage.
- Future candidates may be added without breaking tests that do not own their
  global order or exhaustiveness.
