# 122 -- Query Result Local Sort Fallback

## Problem

Clutch sorts simple table results by rewriting and re-executing the query with
an `ORDER BY`.  That rewrite is deliberately unavailable for UNION, grouped,
derived, and otherwise ambiguous query results, so the shared three-state sort
control failed instead of sorting those rows.

## Decision

Keep server-side sorting for safely rewritable results.  For other tabular
results, use the same unsorted, ascending, descending cycle as a client-side
sort of the currently loaded page.

The local path keeps a shallow snapshot of the page in database-return order.
Changing direction or column always sorts from that snapshot, and clearing the
sort restores it without re-running the query.  Page navigation replaces the
snapshot and applies the active local sort to the newly loaded page.  The
footer and transient label identify the page-local scope.

## Consequence

Header clicks and `s` work consistently for arbitrary query results without
risky SQL rewriting.  Local sorting does not claim a global order across
pages; users still need an explicit SQL `ORDER BY` when whole-result ordering
matters.
