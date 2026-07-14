# 141 — Object Resolution Policy Is Pure

## Problem

Object resolution mixed two different kinds of work in one branch tree.  It
decided whether a symbol should use local candidates, trigger remote discovery,
return a unique hit, open a prefilled picker, or fall back after a miss; at the
same time it performed cache refreshes, liveness checks, messages, and reader
I/O.

The implementation was not unusually large, but its tests had to replace every
effect around every decision.  Six nearby tests used 51 function mocks and
repeated almost the same resolver setup.  That made policy coverage expensive
and allowed the test suite to grow faster than the behavior it protected.

## Decision

Keep the workflow in `clutch-object.el`, but make the resolution policy a pure
decision.  Given the symbol, local candidates, and an optional completed search
result, it chooses one of four plans: search, return, read, or missing.  The
existing resolver remains the only effect boundary and executes that plan.

An attempted search carries an explicit marker.  This distinguishes “search has
not run yet” from “search ran and found nothing” without a sentinel object,
mutable state, or exception-based control flow.  A dead connection supplies the
same attempted-empty input and therefore follows the ordinary missing path
without remote I/O.

No new module, callback, registry, facade, or public API was introduced.  Cache
refresh and backend search remain together because they are one I/O operation;
only their result is passed to the policy.

## Test Budget

The decision matrix is now one table-driven test with no mocks.  One focused
search test proves refresh, merge, type filtering, deduplication, and the
table-only no-refresh rule.  Two small resolver tests retain the real effect
boundary for local/dead behavior and backend error propagation.

The six previous tests became four, their function mocks fell from 51 to 13,
and the object test file lost 89 lines.  Production grew by five lines because
the policy boundary is explicit rather than encoded by nested side effects.

## Consequences

- Local prefix resolution still performs no remote search.
- Single and multiple remote hits, refreshed full candidates, no-hit messaging,
  disconnected fallback, and error propagation keep their prior behavior.
- Future policy cases extend a data table instead of cloning an I/O harness.
- The architecture metrics remain unchanged: 134 cross-module declarations,
  largest SCC two, and root state 14.
