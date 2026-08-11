# 183 — Retire the pg-el NULL parameter workaround

## Context

Clutch temporarily rewrote prepared PostgreSQL placeholders containing `nil` to SQL `NULL` literals because pg-el could not encode an untyped nil Bind value. The workaround also removed those values from the prepared argument list and renumbered every later placeholder. Postmortem [179](179-pg-el-untyped-nil-param-workaround.md) recorded this as compensating adapter code with an explicit removal condition.

Upstream pg-el merged [PR #32](https://github.com/emarsden/pg-el/pull/32) on 2026-08-10. Its public `pg-exec-prepared` path now treats an untyped nil argument as protocol-level SQL NULL, calculates the Bind message length correctly, and writes the required 32-bit NULL length.

## Decision

Delete `clutch-db-pg--rewrite-param-sql-inlining-null` and send every parameter through the ordinary `clutch-db-pg--rewrite-param-sql`, `clutch-db-pg--typed-arguments`, and `pg-exec-prepared` path. Nil therefore remains a bound parameter instead of becoming SQL text, while `:false` remains the text value `"false"` required to distinguish it from Clutch's SQL NULL representation.

Keep the read-side `pg-null-marker` binding and result normalization. Those preserve PostgreSQL boolean false separately from SQL NULL and solve a different boundary problem.

## Why

The upstream protocol library now owns the wire behavior, so retaining a second SQL-rewrite path in Clutch would duplicate its contract and weaken prepared-statement semantics for NULL values. Removing the compensation also eliminates placeholder filtering and renumbering from the adapter.

## Verification

The prepared false/NULL/true unit test now requires SQL `$1`, `$2`, and `$3` plus arguments `(("false") (nil) (t))`, proving that Clutch does not inline or drop the NULL parameter. The existing PostgreSQL live test exercises the same values against the merged upstream pg-el implementation and verifies the resulting row remains `(:false nil t)`.
