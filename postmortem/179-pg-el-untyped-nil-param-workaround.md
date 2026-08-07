# 179 — pg-el untyped nil prepared-parameter workaround

## Context

pg-el's prepared Bind path cannot send an untyped `nil` argument as SQL NULL. The length calculation calls `(string-bytes nil)` and signals `wrong-type-argument stringp nil`, and the wire NULL length 0xFFFFFFFF cannot be emitted bytewise by `pg--send-uint` because `(% -1 256)` is still -1. Typed arguments such as `(nil . "bool")` are handled by pg-el and intentionally serialize as boolean false, so Clutch cannot use a typed nil to mean SQL NULL.

The upstream fix (`a951e7d` "Fix binding of untyped nil parameters as SQL NULL") is open as emarsden/pg-el PR #32 and is not merged. Postmortem 158 already flags the NULL prepared-parameter path as remaining adapter scope.

An earlier `cl-letf` wrapper around `pg-bind` neutralized `format`/`encode-coding-string` for nil, but it was insufficient: upstream pg-bind still crashed in its Bind-message length calculation at `(string-bytes nil)`. A stale duplicate pg-el install (for example an ELPA `pg` package or an old native-compiled `.eln`) surfaced unrelated arity errors on top of that, so that approach was abandoned.

## Decision

Clutch never binds an untyped nil: `clutch-db-pg--rewrite-param-sql-inlining-null` rewrites the nil parameter placeholders to the literal `NULL` and renumbers the remaining placeholders to `$N`, and `clutch-db-execute-params` drops the nil parameters before calling the plain upstream `clutch-db-pg--exec-prepared` path. The placeholder rewrite uses the same `clutch-db-sql-map-placeholders` lexer as ordinary rewriting, so `??`, jsonb's `?|` / `?&`, literals, and comments keep their existing behavior.

The read side is a separate concern: pg-el's `pg-bool-parser` maps `"f"` to `nil` and `"NULL"` to `pg-null-marker`, while Clutch binds `pg-null-marker` to its own marker and normalizes bool-column `nil` to `:false`. That normalization is not part of this workaround and remains after the PR merges.

## Why

Clutch's cross-backend invariant is nil = SQL NULL. Until upstream merges PR #32, nil cannot pass through pg-el's normal Bind path. Inlining `NULL` avoids private pg APIs and global function shadowing, keeps prepared-statement semantics for every non-nil parameter, and is semantically identical to binding a NULL parameter. It works against both upstream main and the fork's fix revision, so Clutch does not depend on the pg-el checkout.

## Verification

Local batch with a stubbed `pg-exec-prepared` drives `clutch-db-execute-params` with `:false`, nil, and `t` parameters: the rewritten SQL is `SELECT $1::bool, NULL::text, $2::bool`, the observed typed arguments are `(("false") (t))`, and the parsed rows keep false, NULL, and true distinct. The full non-live CI suite passes.

## Removal Conditions

When emarsden/pg-el merges PR #32 (or a release includes `a951e7d`), delete `clutch-db-pg--rewrite-param-sql-inlining-null` and the null-inlining branch in `clutch-db-execute-params`, and send all parameters including nil through the plain `clutch-db-pg--exec-prepared` path. Keep the prepared false/NULL contract test as the regression guard. The `:false` read-side normalization and `pg-null-marker` binding remain.
