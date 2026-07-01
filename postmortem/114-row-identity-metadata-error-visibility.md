# 114 -- Row Identity Metadata Error Visibility

## Problem

`clutch--prepare-row-identity-query` tries to enrich simple SELECT queries with
hidden row identity columns so result buffers can support staged edit and delete.
If `clutch-db-row-identity-candidates` signals `clutch-db-error`, the query path
currently keeps the user query running and treats the result as read-only.

That preserves the primary workflow: a transient metadata failure should not
prevent a normal SELECT from showing rows.  The tradeoff is that the row identity
metadata failure is not visible enough; it can look the same as a backend that
does not support editable row identity for that result.

## Better Shape

The result should carry an explicit row identity status, for example:

- available, with the selected candidate and hidden aliases
- unsupported, with a reason such as ambiguous query shape or backend capability
- error, with the metadata error message and diagnostic details

Then ordinary query execution can still display rows while result footer text
and staged edit/delete commands explain why editing is unavailable.  The query
path would no longer need to convert metadata errors into an indistinguishable
nil candidate.

## Why It Was Deferred During Audit

The fix crossed `clutch-query.el`, result rendering, and edit/delete command
gates.  During the package-quality audit, it was safer to record the debt and
then handle it as a focused row identity status refactor.

## Fix

Clutch now keeps the SELECT workflow available while preserving the metadata
failure:

- `clutch--prepare-row-identity-query` records `:identity-status` and
  `:identity-error-message` instead of converting `clutch-db-error` into an
  indistinguishable nil candidate.
- Result buffers keep that status in buffer-local row identity state.
- The footer shows a `row identity error: ...` warning when edit/delete are
  disabled by metadata failure.
- Staged edit/delete entry points report the same metadata error instead of the
  generic "no identity available" message.

This keeps transient metadata failures visible without blocking read-only query
results.

## Adapter Follow-up

A later audit found adapter helpers could still turn row-identity metadata
lookup failures into nil before query preparation saw them.  MySQL,
PostgreSQL, SQLite, and JDBC candidate lookups now translate backend/protocol
errors to `clutch-db-error`; optional describe/comment/FK swallow sites remain
separate design debt.
