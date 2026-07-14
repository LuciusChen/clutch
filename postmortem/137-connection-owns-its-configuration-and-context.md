# 137 — Connection Owns Its Configuration and Buffer Context

## Problem

The entrypoint had become a one-way composition root, but it still defined the
saved-connection registry, TRAMP connection policy, and three buffer-local
values that describe a live connection.  The connection workflow created,
restored, and interpreted all five values while the root merely hosted their
definitions.

That hidden ownership did not appear as a dependency edge, so the SCC and
cross-declaration budgets could remain green while mutable workflow state
accumulated in `clutch.el`.

## Decision

The connection module now defines the state whose lifetime it owns:

- `clutch-connection-alist` and `clutch-tramp-context-policy` are connection
  configuration.
- `clutch-connection`, `clutch--conn-sql-product`, and
  `clutch--connection-params` are connection buffer context.
- The root defines the `clutch` customization group before requiring workflow
  modules, then only assembles them.
- Query and result no longer repeat declarations for connection variables they
  receive through a direct `require`.

The public option names, types, defaults, documentation, and buffer-local
semantics are unchanged.  No accessor facade, callback registry, forwarding
variable, compatibility alias, or new module was introduced.

## Architecture Budget

The architecture checker now counts mutable state definitions in the
composition root: `defcustom`, `defvar`, `defvar-local`, and
`define-minor-mode`.  The current ceiling is 30, down from 35 before this move;
future state cannot silently return to the root even if graph metrics remain
unchanged.

A source-reader test also requires the five connection symbols to have exactly
one owner, `clutch-connection`.  It checks source forms instead of loaded symbol
metadata so stale bytecode and test load order cannot create a false result.

## Consequences

- `clutch.el` fell from roughly 521 lines to 402.
- Root mutable state fell from 35 definitions to 30.
- Cross-module declarations remain 136 and the largest SCC remains three.
- Loading query directly establishes the correct connection context without
  loading the composition root.
- Three unused timeout declarations disappeared from connection; the timeout
  options themselves remain at the root pending a separate contract decision.

## Deferred Boundary

The root still owns 30 mutable definitions.  They should move only when their
workflow owner and public load contract are explicit.  In particular, backend
timeouts and `clutch-result-max-rows` span several workflows and must not be
moved mechanically.  The query/result/edit SCC and duplicated reconnect tests
are separate implementation and test-bloat cuts.
