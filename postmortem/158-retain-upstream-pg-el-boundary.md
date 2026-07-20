# 158 — Retain the upstream pg-el boundary

## Context

Clutch carried connection-local caches that inferred PostgreSQL transaction state from the SQL it sent and rejected array literals with explicit dimension bounds before they reached the server.  Those compensations existed because pg-el did not expose the `ReadyForQuery` transaction byte and could not parse valid dimension-prefixed array results.

Upstream pg-el merged `pgcon-transaction-status` and dimension-prefixed array parsing on 2026-07-18.  The two Clutch compensations can therefore be removed in favor of tested upstream behavior.

## Decision

Clutch uses upstream pg-el as the protocol boundary for `:backend pg`.

Clutch reads transaction state directly from the public `pgcon-transaction-status` accessor and treats PostgreSQL's `I`, `T`, and `E` `ReadyForQuery` bytes as authoritative.  Clutch no longer maintains parallel open/failed transaction caches or infers their state from SQL text and query outcomes.

Dimension-prefixed array literals are passed through as PostgreSQL array syntax, and array results rely on pg-el's parser.  Clutch retains only caller-owned behavior: manual-mode preference, lazy `BEGIN`, transaction commands, parameter presentation, metadata SQL, and result normalization.

The adapter fails clearly at connection time when the installed pg-el predates `pgcon-transaction-status`.  It does not add a compatibility cache or a second transaction-state path.

## Why

The upstream API now supplies the server-authoritative state Clutch needs.  Keeping a second inferred state model would create two sources of truth and could drift after explicit transaction SQL, server errors, cancellation, or protocol recovery.

Using the upstream public API keeps the change narrow and removes code from Clutch while preserving the already exercised authentication, TLS, type, and cancellation paths.  Protocol behavior remains owned and tested at the dependency boundary instead of being inferred in the caller.

## Remaining adapter scope

Other pg-el adapter code is not removed merely because it looks protocol-adjacent.  In particular, the NULL prepared-parameter path remains until pg-el provides the inferred-type protocol NULL semantics Clutch needs.  Each remaining compensation should be retired only when a tested upstream public contract replaces it.

## Consequences

- PostgreSQL transaction UI follows the actual server state rather than Clutch's estimate.
- Explicit-bound one-dimensional arrays work without a Clutch rejection path; pg-el still intentionally discards lower-bound metadata and retains its multidimensional-array limitation.
- Native PostgreSQL users must update pg-el to a revision containing the merged public accessor and parser changes.
- Clutch main becomes smaller while keeping a single PostgreSQL protocol boundary.
